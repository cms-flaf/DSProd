"""DSProd law task framework: base Task + HTCondor workflow.

Phase 1 provides the generic scaffolding shared by every production task:
  * `Task` — loads the production setup (points via the process module) and the per-era
    conditions, and provides storage/target helpers.
  * `HTCondorWorkflow` (+ kerberos-renewing proxy) — long CMSSW jobs on HTCondor.
The concrete production tasks (MakeGridpack, RunProd, NanoMergeTask, MakeManifest) are
added in later phases and reuse these bases.
"""

import contextlib
import copy
import datetime
import fnmatch
import glob
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import law
import luigi
import yaml

from . import gridpack_store, registry, run_step
from .config import get_global
from .crab import CrabWorkflow
from .law_wlcg import WLCGFileSystem, WLCGFileTarget
from .tools import (
    CreateVomsProxy,
    ResyncExistingBranchesProxy,
    StopOnMassInitialRetryProxy,
    on_batch_node,
    ps_call,
    submitted_task_family,
    timed_call_wrapper,
    update_kerberos_ticket,
)

law.contrib.load("htcondor")

#: path prefixes that must be served by a remote (WLCG/gfal) target
_REMOTE_PREFIXES = ("davs://", "root://", "gsiftp://", "/eos/")

#: lazily-built default file system (a remote one needs a valid VOMS proxy at construction time).
_fs_default = None


def get_fs():
    """The default file system, from `fs_default` in the global/user config (FLAF notation): one
    URI carrying protocol, host and base path, e.g.
    `davs://eoshome-k.cern.ch:8444/eos/user/k/kandroso/DSProd/`. A plain `/...` path gives a local
    file system instead. Remote access goes through the gfal-CLI interface, which also works on
    CRAB workers (where the gfal2 python module law.contrib.gfal needs is unavailable).

    Every backend (local, htcondor, crab) writes to this one file system.
    """
    global _fs_default
    if _fs_default is None:
        base = get_global().get("fs_default")
        if not base:
            raise RuntimeError(
                "No default file system defined. Please define `fs_default` in "
                "config/user_custom.yaml, e.g.\n"
                "  fs_default: davs://eoshome-k.cern.ch:8444/eos/user/k/kandroso/DSProd/"
            )
        _fs_default = (
            law.LocalFileSystem(base=base)
            if base.startswith("/")
            else WLCGFileSystem(base)
        )
    return _fs_default


def copy_param(ref_param, new_default):
    param = copy.deepcopy(ref_param)
    param._default = new_default
    return param


def is_remote_path(path):
    return path.startswith(_REMOTE_PREFIXES)


def select_by_pattern(values, patterns, option, setup, key=lambda v: v):
    """Filter `values` by fnmatch `patterns`; no patterns keeps everything.

    Matching nothing is an error: a run that silently produces nothing is worse than one that
    refuses to start.
    """
    if not patterns:
        return list(values)
    selected = [v for v in values if any(fnmatch.fnmatch(key(v), p) for p in patterns)]
    if not selected:
        raise RuntimeError(f"{option} {','.join(patterns)} matches nothing in {setup}")
    return selected


def _git_head(path):
    """Commit a checkout is on, for provenance records ("unknown" outside a git checkout)."""
    try:
        out = subprocess.run(
            ["git", "-C", path or ".", "rev-parse", "HEAD"],
            capture_output=True,
            check=True,
        )
        return out.stdout.decode().strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def runprod_branches(eras, points):
    """Ordered (era, point_index, seed) list — the single source of RunProd branch numbering.

    Shared by RunProd.create_branch_map and NanoMergeTask (which inverts it to find the RunProd
    branch id of each seed it merges), so the two never drift.
    """
    out = []
    for era in eras:
        for pi, point in enumerate(points):
            for job in range(point.n_jobs(era)):
                out.append((era, pi, job + 1))  # seed = 1-based job index
    return out


def merge_groups(seeds, files_per_merge):
    """Ordered (group index, seeds) pairs — the single source of NanoMergeTask's grouping.

    Shared by `NanoMergeTask.create_branch_map` and `BackfillProducedRecords` (which needs to
    know which seeds a merged file accounts for), so the two never drift.
    """
    return [
        (group, seeds[start : start + files_per_merge])
        for group, start in enumerate(range(0, len(seeds), files_per_merge))
    ]


def premix_dataset_for_era(conditions, era):
    """The `dbs:` pileup dataset an era's steps resolve, or None when it has no premix step."""
    for step in conditions[era]["prod_steps"]:
        params = run_step.resolve_step_params(conditions, era, step)
        pileup = str(params.get("pileup_input") or "")
        if pileup.startswith("dbs:") or pileup.startswith("das:"):
            return pileup[4:]
    return None


def cmssw_releases_for_era(conditions, era):
    """Sorted unique (SCRAM_ARCH, CMSSW) needed to produce `era` (all prod_steps + NANO versions)."""
    releases = set()
    for step in conditions[era]["prod_steps"]:
        if step == "NANO":
            for version in conditions[era]["NANO"].get("versions", {}):
                p = run_step.resolve_step_params(
                    conditions, era, "NANO", version=version
                )
                releases.add((p["SCRAM_ARCH"], p["CMSSW"]))
                # the merge may run in a different release (haddnano.py), which must be installed
                m = run_step.merge_params(p)
                releases.add((m["SCRAM_ARCH"], m["CMSSW"]))
        else:
            p = run_step.resolve_step_params(conditions, era, step)
            releases.add((p["SCRAM_ARCH"], p["CMSSW"]))
    return sorted(releases)


class Task(law.Task):
    setup = luigi.Parameter(description="path to the production setup YAML")
    eras = law.CSVParameter(
        default=(),
        description="produce only the eras matching one of these patterns (fnmatch globs, e.g. "
        "'Run3_2023*'); default: every era of the setup",
    )
    points = law.CSVParameter(
        default=(),
        description="produce only the points whose name matches one of these patterns "
        "(fnmatch globs, e.g. '*_M-800'); default: every point of the setup",
    )
    test = luigi.IntParameter(
        default=0,
        description="test mode: produce this many events per point and era in a single job, "
        "into a separate `<output>_test` area; 0 (default) = full production",
    )

    # class-level cache: a single setup is loaded once per process
    setup_path = None
    prod_setup = None
    conditions = None
    process = None
    all_points = None

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        setup_path = self.to_abs(self.setup)
        if Task.setup_path is None:
            with open(setup_path, "r") as f:
                Task.prod_setup = yaml.safe_load(f)
            cond_path = self.to_abs(Task.prod_setup["conditions"])
            with open(cond_path, "r") as f:
                Task.conditions = yaml.safe_load(f)
            Task.process = registry.get_process(Task.prod_setup["process"])
            Task.all_points = Task.process.enumerate_points(Task.prod_setup)
            Task.setup_path = setup_path
            if self.test == 0:
                self._validate_merge_granularity()
        if setup_path != Task.setup_path:
            raise RuntimeError(
                f"Inconsistent setup path: {setup_path} != {Task.setup_path}"
            )
        self.prod_setup = Task.prod_setup
        self.conditions = Task.conditions
        self.process = Task.process
        self.prod_eras = select_by_pattern(
            Task.prod_setup["eras"], self.eras, "--eras", self.setup
        )
        self.prod_points = self._select_points(Task.all_points)
        _, setup_full_name = os.path.split(setup_path)
        self.setup_name, _ = os.path.splitext(setup_full_name)

    def _validate_merge_granularity(self):
        """Refuse a setup whose samples would not fill whole merged files.

        `NanoMergeTask` groups `files_per_merge` production files into one product, so a sample is
        delivered as `events_total / (events_per_job * files_per_merge)` files. When that does not
        divide, the last group is short and the sample becomes N full files plus a stub -- which
        makes a later top-up production awkward to reason about, since "how many more files do I
        need" no longer has a whole-number answer. Checked on the setup itself, so any task refuses
        it, not only the merge. `--test` is exempt: it deliberately produces a single short job.
        """
        wanted = int(self.prod_setup["events_per_job"])
        per_file = wanted * int(self.prod_setup.get("files_per_merge", 20))
        # The validation below divides by the setup's job size, while `RunProd` produces
        # `point.events_per_job` events and `n_jobs()` counts seeds with it. A point carrying its
        # own size would therefore be validated against a number it does not use, and deliver
        # merged files of a size nobody asked for.
        resized = sorted(
            {
                (point.name, int(point.events_per_job or 0))
                for point in Task.all_points
                if int(point.events_per_job or 0) != wanted
            }
        )
        if resized:
            shown = ", ".join(f"{name} ({size})" for name, size in resized[:6])
            more = f" ... and {len(resized) - 6} more" if len(resized) > 6 else ""
            raise RuntimeError(
                f"{self.setup}: events_per_job is a property of the setup ({wanted}), but "
                f"{len(resized)} point(s) carry a different one: {shown}{more}. The merge "
                "contract is one file per events_per_job x files_per_merge events, so a "
                "per-point size silently changes the size of the delivered files."
            )
        bad = [
            f"{point.name} / {era}: {n} events"
            for point in Task.all_points
            for era, n in sorted(point.events_total.items())
            if n and n % per_file
        ]
        if bad:
            shown = "\n  ".join(bad[:8])
            more = f"\n  ... and {len(bad) - 8} more" if len(bad) > 8 else ""
            raise RuntimeError(
                f"{self.setup}: events_total must be a multiple of events_per_job x "
                f"files_per_merge ({per_file}), otherwise the last merged file of a sample is "
                f"incomplete:\n  {shown}{more}"
            )

    def _select_points(self, points):
        """Apply `--points`, the era selection and `--test`.

        Selecting a subset only narrows what this run produces: output paths are keyed by era,
        point name and seed, never by branch id, so a selective run writes exactly where the full
        production would.
        """
        points = select_by_pattern(
            points, self.points, "--points", self.setup, key=lambda p: p.name
        )
        # a point that produces nothing in the selected eras is not part of this run at all —
        # its gridpack would otherwise be prepared for no reason
        points = [p for p in points if any(p.n_events(e) > 0 for e in self.prod_eras)]
        if not points:
            raise RuntimeError(
                f"no selected point of {self.setup} produces events in eras "
                f"{','.join(self.prod_eras)}"
            )
        if self.test > 0:
            # one short job per point and era, wherever the setup produces that point
            points = [
                replace(
                    p,
                    events_total={
                        era: self.test for era, n in p.events_total.items() if n > 0
                    },
                    events_per_job=self.test,
                )
                for p in points
            ]
        return points

    # ---- setup accessors ----------------------------------------------------
    def gridpack_points(self):
        """One representative point per *distinct* gridpack, in setup order.

        Points sharing a gridpack (same `gridpack_name`) collapse to one entry — the single source
        of MakeGridpack branch numbering, mirrored by `gridpack_index()`.
        """
        seen = {}
        for point in self.prod_points:
            seen.setdefault(self.process.gridpack_name(point), point)
        return list(seen.values())

    def gridpack_index(self):
        """gridpack name -> MakeGridpack branch id."""
        return {
            self.process.gridpack_name(p): i
            for i, p in enumerate(self.gridpack_points())
        }

    def nano_versions(self, era):
        """NanoAOD versions to produce for `era` (per-era override, else global default)."""
        nv = self.prod_setup.get("nano_versions", {})
        if isinstance(nv, dict):
            return nv.get(era, nv.get("default", []))
        return nv

    # ---- path / target helpers ---------------------------------------------
    def ana_path(self):
        return os.getenv("ANALYSIS_PATH")

    def ana_data_path(self):
        return os.getenv("ANALYSIS_DATA_PATH")

    def to_abs(self, path):
        if len(path) == 0:
            return self.ana_path()
        if path[0] == "/" or is_remote_path(path):
            return path
        return os.path.join(self.ana_path(), path)

    def store_parts(self):
        """Local (job/bookkeeping) directory of this run.

        A point selection and `--test` renumber the branches, so they get their own directory —
        otherwise law would match this run's job data against a differently-numbered one.
        """
        name = self.setup_name
        for patterns in (self.eras, self.points):
            if patterns:
                joined = ",".join(sorted(patterns))
                slug = re.sub(r"\W+", "", "".join(sorted(patterns)))[:24]
                name += f"_{slug}{hashlib.sha1(joined.encode()).hexdigest()[:6]}"
        if self.test > 0:
            name += f"_test{self.test}"
        return (self.__class__.__name__, name)

    def local_path(self, *path):
        parts = (self.ana_data_path(),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))

    def remote_target(self, *path, fs=None):
        """A target on `fs` (default: `fs_default`). `path` is relative to the file system's base,
        as in FLAF — the base lives in the fs, not in the path."""
        fs = fs or get_fs()
        path = os.path.join(*path)
        if isinstance(fs, law.LocalFileSystem):
            return law.LocalFileTarget(path, fs=fs)
        return WLCGFileTarget(path, fs=fs)

    def prod_storage_name(self):
        """Top-level product directory of the *production* on `fs_default` (setup `output`)."""
        return self.prod_setup.get("output", self.setup_name)

    def storage_name(self):
        """Top-level product directory of this run.

        `--test` appends `_test`, so a short test run can never overwrite a production sample.
        """
        name = self.prod_storage_name()
        return f"{name}_test" if self.test > 0 else name

    def storage_path(self, *parts):
        """Path of a product relative to `fs_default`: <storage name>/<parts...>."""
        return os.path.join(self.storage_name(), *parts)

    def staged_nano_target(self, era, point, version, seed):
        """The per-seed nano file `RunProd` stages for `NanoMergeTask` to consume.

        Deliberately not what `RunProd` declares as its output: the merge deletes this file, and
        an output that disappears is read as work to redo (see `produced_nano_target`).
        """
        name = self.process.point_name(point)
        return self.storage_target(
            "staging", f"nanoAOD_{version}", era, name, f"nano_{version}_{seed}.root"
        )

    def produced_nano_target(self, era, point, version, seed):
        """The record that a seed's nano file was produced -- `RunProd`'s actual output.

        `NanoMergeTask` deletes each staged nano file once it has merged and verified it, so the
        file's presence cannot say whether the seed ran: a resumed workflow found the outputs of
        every already-merged seed missing and resubmitted the whole era (law marks such jobs
        "initially missing task outputs"). Nothing deletes this record, so completeness survives
        the merge. To redo a seed on purpose, delete its record along with its nano file.
        """
        name = self.process.point_name(point)
        return self.storage_target(
            "produced", f"nanoAOD_{version}", era, name, f"nano_{version}_{seed}.json"
        )

    def merged_nano_target(self, era, point, version, group):
        """The merged nano file `NanoMergeTask` writes for one group of seeds."""
        name = self.process.point_name(point)
        return self.storage_target(
            f"nanoAOD_{version}", era, name, f"nano_{version}_{group}.root"
        )

    def _write_produced_record(self, era, point, version, seed, n_events):
        """Record that this seed's nano file exists, so its merge can safely delete the file."""
        record = {
            "era": era,
            "point": self.process.point_name(point),
            "version": version,
            "seed": seed,
            "events_requested": int(n_events),
            "staged": self.staged_nano_target(era, point, version, seed).uri(),
        }
        with self.produced_nano_target(era, point, version, seed).localize(
            "w"
        ) as out_local:
            with open(out_local.abspath, "w") as f:
                json.dump(record, f)

    def premix_target(self, era):
        """Where an era's premix file list lives on `fs_default`.

        Always the production area, also under `--test`: the list depends only on the era.
        """
        return self.remote_target(self.prod_storage_name(), "premix", f"{era}.txt")

    def gridpack_target(self, gridpack_name):
        """Where a gridpack lives on `fs_default`.

        Always the production area, also under `--test`: a gridpack does not depend on the number
        of events, so a test run reuses the production one instead of regenerating it.
        """
        return self.remote_target(
            os.path.join(
                self.prod_storage_name(), "gridpacks", gridpack_name, "gridpack.tar.xz"
            )
        )

    def storage_target(self, *parts):
        return self.remote_target(self.storage_path(*parts))

    def law_job_home(self):
        """Scratch directory for a task's intermediate files, and whether we own it.

        A batch job gets the one law prepared. A local run uses local disk -- `$DSPROD_SCRATCH` if
        set, else the system temp dir -- never `data/`: the production chain writes multi-GB
        intermediates there, and when `data/` sits on an EOS mount a transient failure to open one
        of them locally sends cmsRun to the xrootd fallback with a mangled path
        ("Opening relative path '?tried=' is disallowed"), losing the whole chain at the last step.
        """
        if "LAW_JOB_HOME" in os.environ:
            return os.environ["LAW_JOB_HOME"], False
        base = os.environ.get("DSPROD_SCRATCH") or tempfile.gettempdir()
        os.makedirs(base, exist_ok=True)
        return tempfile.mkdtemp(dir=base), True


class HTCondorWorkflowProxy(
    ResyncExistingBranchesProxy,
    StopOnMassInitialRetryProxy,
    law.htcondor.workflow.HTCondorWorkflowProxy,
):
    def __init__(self, *args, **kwargs):
        super(HTCondorWorkflowProxy, self).__init__(*args, **kwargs)
        if self.task.krenew > 0:
            self.kerberos_update = timed_call_wrapper(
                update_kerberos_ticket, self.task.krenew * 60 * 60, verbose=1
            )

    def poll(self):
        self.kerberos_update()
        super(HTCondorWorkflowProxy, self).poll()


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    # law copies parameter values from one task to another in `req()`, so a resource request would
    # otherwise leak into everything a task requires. `NanoMergeTask` (3 h, 1 CPU) forced its
    # `RunProd` requirement (24 h, 4 CPUs) to run in 3 h on a single core, and 91 % of a 3270-job
    # CRAB task was killed on walltime. Workflow <-> branch conversion passes `_skip_task_excludes`,
    # so a value given on the command line still reaches the branches of the task it was given for.
    #
    # The failure budget (`RunProd.retries` / `RunProd.tolerance`) leaks the same way, and worse:
    # `NanoMergeTask` carries law's defaults for both, so driving a production through the merge
    # handed `RunProd` `tolerance=0.0` -- one job out of retries then killing the whole run -- and
    # would silently drop a `--RunProd-retries` given on the command line.
    exclude_params_req = law.htcondor.HTCondorWorkflow.exclude_params_req | {
        "max_runtime",
        "n_cpus",
        "retries",
        "tolerance",
    }

    max_runtime = law.DurationParameter(
        default=24.0,
        unit="h",
        significant=False,
        description="maximum runtime, default unit is hours",
    )
    n_cpus = luigi.IntParameter(
        default=1, significant=False, description="number of CPU slots"
    )
    krenew = luigi.IntParameter(
        default=1, significant=False, description="call 'kinit -R' each krenew hours"
    )
    poll_interval = copy_param(law.htcondor.HTCondorWorkflow.poll_interval, 5)

    workflow_proxy_cls = HTCondorWorkflowProxy

    def htcondor_output_directory(self):
        return law.LocalDirectoryTarget(self.local_path())

    def htcondor_bootstrap_file(self):
        return os.path.join(self.ana_path(), "bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):
        config.render_variables["analysis_path"] = self.ana_path()
        config.custom_content.append(
            ("requirements", '(TARGET.OpSysAndVer =?= "AlmaLinux9")')
        )
        config.custom_content.append(
            ("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1)
        )
        n_cpus = int(self.n_cpus)
        if n_cpus > 1:
            config.custom_content.append(("RequestCpus", n_cpus))

        log_path = os.path.abspath(os.path.join(self.ana_data_path(), "logs"))
        os.makedirs(log_path, exist_ok=True)
        config.custom_content.append(
            ("log", os.path.join(log_path, "job.$(ClusterId).$(ProcId).log"))
        )
        config.custom_content.append(
            ("output", os.path.join(log_path, "job.$(ClusterId).$(ProcId).out"))
        )
        config.custom_content.append(
            ("error", os.path.join(log_path, "job.$(ClusterId).$(ProcId).err"))
        )
        return config


class InstallCMSSW(Task, law.LocalWorkflow):
    """Install (once, on the shared AFS area) the CMSSW releases an era's production needs.

    Runs locally (installs into `soft/`, which HTCondor workers see via AFS); the releases depend on
    the era, so this is a per-era branch. Idempotent — env.sh guards each release with a .installed flag.
    """

    def create_branch_map(self):
        return dict(enumerate(self.prod_eras))

    def output(self):
        return self.local_target(f"{self.branch_data}.installed")

    def run(self):
        era = self.branch_data
        for arch, cmssw in cmssw_releases_for_era(self.conditions, era):
            print(f"InstallCMSSW[{era}]: installing {cmssw} ({arch})")
            ps_call(
                [f"bash {self.ana_path()}/env.sh install {arch} {cmssw}"],
                shell=True,
                verbose=1,
            )
        out = self.output()
        os.makedirs(os.path.dirname(out.path), exist_ok=True)
        out.touch()


class GridpackTask(Task):
    """Shared branch map and output location of the gridpack-providing tasks.

    Branches over *distinct* gridpacks, not points: several points can share one gridpack (e.g.
    the final states of X->HH->bbWW, where the Higgses leave the generator undecayed), and one
    branch per point would make them race on the same output.
    """

    def create_branch_map(self):
        return dict(enumerate(self.gridpack_points()))

    def gridpack_rel(self):
        """Path of this branch's gridpack inside the DSProdGridpacks store."""
        return self.process.gridpack_rel_path(self.branch_data)

    def output(self):
        return self.gridpack_target(self.process.gridpack_name(self.branch_data))


class ImportGridpack(GridpackTask, law.LocalWorkflow):
    """Copy a gridpack that the DSProdGridpacks store already has to `fs_default`.

    Always local: it needs the git checkout and the Git-LFS server, neither of which a grid worker
    has. It is complete — nothing to do — when the gridpack is already on `fs_default` or when the
    store does not have it, in which case `MakeGridpack` generates it instead.
    """

    def store_root(self):
        return gridpack_store.store_root(self.ana_path())

    def complete(self):
        if self.is_workflow():
            return super(ImportGridpack, self).complete()
        return self.output().exists() or not gridpack_store.contains(
            self.store_root(), self.gridpack_rel()
        )

    def run(self):
        rel = self.gridpack_rel()
        with self.output().localize("w") as out_local:
            if not gridpack_store.fetch(self.store_root(), rel, out_local.abspath):
                raise RuntimeError(
                    f"could not materialize {rel} from the gridpack store"
                )


class MakeGridpack(GridpackTask, HTCondorWorkflow, CrabWorkflow, law.LocalWorkflow):
    """Generate each *distinct* gridpack that neither `fs_default` nor the store already has.

    Importing from the store is `ImportGridpack`'s job and has run by the time this task decides
    what to submit, so a branch reaching `run()` really has to be generated.
    """

    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 12.0)

    def workflow_requires(self):
        reqs = super(MakeGridpack, self).workflow_requires()
        reqs["store"] = ImportGridpack.req(self, workflow="local")
        return reqs

    def requires(self):
        return {
            "store": ImportGridpack.req(self, branch=self.branch, workflow="local"),
        }

    def run(self):
        # Generating on the grid is fine -- that is what submitting MakeGridpack does. What must
        # not happen is a *production* job quietly building its own gridpack after finding it
        # missing (often only because the worker cannot reach `fs_default`): that costs the
        # production slot ~1.5 h of MadGraph and the upload then fails from the same worker.
        # If the submitted task cannot be determined, allow it rather than block a legitimate run.
        family = self.get_task_family().rsplit(".", 1)[-1]
        submitted = submitted_task_family()
        if on_batch_node() and submitted not in (None, family):
            raise RuntimeError(
                f"gridpack '{self.process.gridpack_name(self.branch_data)}' is missing or "
                f"unreadable from fs_default, and this job was submitted to run {submitted}, not "
                f"{family}: a production job must not build a gridpack on its own slot. Submit "
                f"{family} itself -- it runs on the grid just as well -- or check that fs_default "
                "is reachable from the worker, then resubmit."
            )
        self._generate(self.process.gridpack(self.branch_data))

    def _generate(self, spec):
        """Render the process cards and run genproductions_scripts gridpack_generation.sh."""
        point = self.branch_data
        work_dir, is_tmp = self.law_job_home()
        try:
            cards_dir = os.path.join(work_dir, "cards")
            name = self.process.render_gridpack_cards(point, cards_dir)
            gen_bin = os.path.join(
                self.ana_path(), "genproductions_scripts", "bin", spec.generator
            )
            # gridpack_generation.sh reconstructs its Utilities path assuming the repo is named
            # `genproductions`; in genproductions_scripts that lookup fails. Make Utilities reachable
            # via the script's fallback (<bin>/Utilities) and pass PRODHOME explicitly (line 776
            # only defaults PRODHOME to pwd when unset). The gridpack is built in cwd (work_dir).
            util_link = os.path.join(gen_bin, "Utilities")
            if not os.path.exists(util_link):
                try:
                    os.symlink(os.path.join("..", "..", "Utilities"), util_link)
                except FileExistsError:
                    pass
            # gridpack_generation.sh sets up its OWN CMSSW and aborts if a CMSSW environment is
            # already active (it checks $CMSSW_BASE). On grid (CRAB) workers our env.sh sets up a
            # cvmfs CMSSW to provide python3.9 for law, so strip the CMSSW / SCRAM / python-
            # injection vars from the child env to give the script a clean shell. This is a no-op
            # on lxplus, where DSProd's env sets up no CMSSW.
            env = dict(os.environ, PRODHOME=gen_bin)
            for var in (
                "CMSSW_BASE",
                "CMSSW_VERSION",
                "CMSSW_RELEASE_BASE",
                "CMSSW_FWLITE_INCLUDE_PATH",
                "CMSSW_SEARCH_PATH",
                "CMSSW_DATA_PATH",
                "LOCALRT",
                "PYTHONPATH",
                "PYTHONHOME",
            ):
                env.pop(var, None)
            ps_call(
                [f"bash {gen_bin}/gridpack_generation.sh {name} {cards_dir}"],
                shell=True,
                cwd=work_dir,
                env=env,
                verbose=1,
            )
            tarballs = glob.glob(os.path.join(work_dir, f"{name}_*_tarball.tar.xz"))
            if not tarballs:
                raise RuntimeError(f"gridpack tarball not produced for {name}")
            with self.output().localize("w") as out_local:
                shutil.copy(tarballs[0], out_local.abspath)
        finally:
            if is_tmp:
                shutil.rmtree(work_dir, ignore_errors=True)


class CollectGridpacks(Task):
    """Collect the gridpacks this setup produced into the local DSProdGridpacks checkout.

    For each distinct gridpack of the setup:

    * already tracked by the store — nothing to do;
    * otherwise present on `fs_default` (so it was generated here) — download it into the store
      checkout together with a `README.md` recording how it was produced;
    * neither — reported as not produced yet.

    Nothing is committed: adding ~30 MB of Git-LFS content per gridpack stays a deliberate act, so
    the task ends by printing the `git add --sparse` / `commit` / `push` commands to run.
    """

    def output(self):
        return self.local_target("collected.json")

    def complete(self):
        # a collection step, not a product: re-run it whenever it is asked for
        return False

    def _readme(self, point, path, rel):
        """Provenance for a gridpack generated here (the store requires one per gridpack)."""
        name = self.process.gridpack_name(point)
        params = ", ".join(f"`{k}` = {v}" for k, v in sorted(point.params.items()))
        spec = self.process.gridpack(point)
        cards = os.path.relpath(
            spec.cards_template, os.path.join(self.ana_path(), "models")
        )
        repos = "\n".join(
            f"| {repo} | `{_git_head(os.path.join(self.ana_path(), sub))}` |"
            for repo, sub in (
                ("DSProd", ""),
                ("DSProdModels (`models/`)", "models"),
                ("genproductions_scripts", "genproductions_scripts"),
            )
        )
        return f"""# {name}

Point parameters: {params or "none"}.

## Origin

**Generated by DSProd** (setup `{self.setup_name}`) on {datetime.date.today().isoformat()}, with
`genproductions_scripts/bin/{spec.generator}/gridpack_generation.sh` from the cards in
[DSProdModels `{cards}`](https://github.com/cms-flaf/DSProdModels/tree/main/{cards}).

Exact code it was produced with:

| repository | commit |
|---|---|
{repos}

| | |
|---|---|
| size | {os.path.getsize(path)} bytes |
| sha256 | `{gridpack_store.sha256sum(path)}` |
| collected from | `{self.gridpack_target(name).path}` on `fs_default` |
"""

    def run(self):
        root = gridpack_store.store_root(self.ana_path())
        if not gridpack_store.is_available(root):
            raise RuntimeError(
                f"the gridpack store is not checked out at {root}; run ./setup_gridpacks.sh"
            )

        rows, collected = [], []
        for point in self.gridpack_points():
            name = self.process.gridpack_name(point)
            rel = self.process.gridpack_rel_path(point)
            if gridpack_store.contains(root, rel):
                rows.append((name, "in store", rel))
                continue
            target = self.gridpack_target(name)
            if not target.exists():
                rows.append((name, "not produced", rel))
                continue
            dest = gridpack_store.local_path(root, rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with target.localize("r") as inp:
                shutil.copy(inp.abspath, dest)
            readme = os.path.join(os.path.dirname(dest), "README.md")
            if not os.path.exists(readme):
                with open(readme, "w") as f:
                    f.write(self._readme(point, dest, rel))
            rows.append((name, "collected", rel))
            collected.append(rel)

        width = max(len(r[0]) for r in rows) if rows else 0
        print(f"\nGridpack store: {root}")
        for name, status, _ in rows:
            print(f"  {name:<{width}}  {status}")
        if collected:
            print(
                f"\n{len(collected)} gridpack(s) copied into the store. To commit them:\n"
            )
            for cmd in gridpack_store.git_add_hint(root, collected):
                print(f"  {cmd}")
            print(
                "\n`--sparse` is required: the store is checked out sparsely, and a plain "
                "`git add` skips the tarballs."
            )
        else:
            print("\nNothing new to commit.")

        self.output().parent.touch()
        self.output().dump(
            {
                "setup": self.setup_name,
                "store": root,
                "gridpacks": [{"name": n, "status": s, "path": p} for n, s, p in rows],
            },
            indent=2,
            formatter="json",
        )


class PremixFileList(Task, law.LocalWorkflow):
    """Resolve an era's premix pileup dataset to a file list, once, and store it on `fs_default`.

    `cmsDriver --pileup_input dbs:<dataset>` resolves the dataset with a DAS query *in the job*.
    That dataset holds ~38 000 files, and a production asks every job to look it up: at a few
    thousand concurrent jobs the queries start coming back empty, cmsDriver writes a config with no
    secondary input, and cmsRun dies with

        NoSecondaryFiles: RootEmbeddedFileSequence no input files specified for secondary input

    after the job has already produced its GEN-SIM. Resolving once here and passing the result as
    `filelist:` gives every job exactly what a successful DAS query would have, with no per-job
    lookup. Always local: a worker is the last place that query should run.
    """

    def create_branch_map(self):
        eras = [
            era
            for era in self.prod_eras
            if premix_dataset_for_era(self.conditions, era)
        ]
        return dict(enumerate(eras))

    def output(self):
        return self.premix_target(self.branch_data)

    def run(self):
        era = self.branch_data
        dataset = premix_dataset_for_era(self.conditions, era)
        family = self.get_task_family().rsplit(".", 1)[-1]
        if on_batch_node() and submitted_task_family() not in (None, family):
            raise RuntimeError(
                f"the premix file list for {era} is missing from fs_default, and resolving it "
                f"needs a DAS query that must not run on a worker. Submit {family} (or any task "
                "that requires it) from a machine with dasgoclient, then resubmit."
            )
        _, out, _ = ps_call(
            [f'dasgoclient -query="file dataset={dataset}"'],
            shell=True,
            catch_stdout=True,
            verbose=1,
        )
        files = [
            line.strip() for line in out.splitlines() if line.strip().endswith(".root")
        ]
        if not files:
            raise RuntimeError(
                f"DAS returned no files for the premix dataset {dataset}"
            )
        print(f"PremixFileList[{era}]: {len(files)} files from {dataset}")
        with self.output().localize("w") as out_local:
            with open(out_local.abspath, "w") as f:
                f.write("\n".join(files) + "\n")


class RunProd(Task, HTCondorWorkflow, CrabWorkflow, law.LocalWorkflow):
    """Fused GEN->NANO production for one (era, point, seed); stages one nano per version."""

    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 24.0)
    # 4 cores, i.e. 10 GB on CRAB (2500 MB per core, which is also its cap for four). Measured
    # over a finished 4800-job era: the median job runs 7.07 h on two cores and 4.38 h on four,
    # for 34 % more core-hours. It buys throughput, not a shorter tail -- the slowest 1 % of jobs
    # run at cpu/wall 0.28 and barely move (p99 1.07x) -- so drop this line if the core-hours are
    # worth more than the wall-clock. A single-threaded job of this chain peaked at 3042 MB, above
    # what a one-core slot offers, so one core is not an option.
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 4)

    # 4 attempts per job: law submits a job once and then resubmits it `retries` times, so the
    # budget a branch really burns is `retries + 1`. (It then offers the exhausted job to the
    # submission step one last time and ignores whatever that job reports, so the number of CRAB
    # jobs is one higher again; only the budgeted attempts decide when a branch counts as failed.)
    # Every attempt after the first costs a generation of wall clock -- a job of this chain runs
    # 7.1 h at the median -- so law's default of 5 buys a broken branch six generations, ~2 days,
    # before it is finally called failed. Four is enough to walk away from a black-hole site
    # (whose own quarantine needs 5 failures at that site to fire) while a branch that keeps dying
    # is called failed in roughly a day.
    retries = copy_param(HTCondorWorkflow.retries, 3)
    # ... and 5 % of the branches may end up out of attempts without stopping the run (law reads
    # a tolerance at or below 1 as a fraction, so a production of fewer than 20 branches is still
    # ended by its first dead branch and needs an absolute `--RunProd-tolerance 2`). law's
    # default of 0.0 means the FIRST branch to burn its budget raises `tolerance exceeded` and
    # takes a multi-day production with it, 4799 finished jobs and all -- and with a 45-minute
    # retry release window and 56 % of failures arriving in under 6 min, one bad site can spend a
    # branch's four attempts in an afternoon. Only a branch that burns all four counts here, which
    # a failing site rarely produces on its own (one host failed 258 of 3270 jobs and nearly all
    # of them succeeded on a retry elsewhere), so 5 % is room for the unlucky ones; a production
    # that is broken everywhere still stops within the hour, since every one of its branches is
    # out of attempts by then.
    #
    # `acceptance` stays at law's 1.0, and that -- not `tolerance` -- is what forbids a silently
    # short sample: the run keeps going past a failed branch, but once no job is left to finish,
    # law reports `acceptance of N not reached` and the workflow fails. Rerunning it resubmits
    # exactly the failed branches with a fresh budget (law keeps `_job_retries` in memory only).
    tolerance = copy_param(HTCondorWorkflow.tolerance, 0.05)

    def create_branch_map(self):
        return dict(enumerate(runprod_branches(self.prod_eras, self.prod_points)))

    def workflow_requires(self):
        # `req_different_branching` and not `req`: these branch over gridpacks and eras, not over
        # seeds, and law copies `branches` through `req()`. `--branches 10:20` then asked for
        # gridpacks 10-19 while seed 10 needs gridpack 0, so the requirement was satisfied by a
        # workflow that never builds the gridpack those seeds need, and it dropped the premix list
        # of every era outside the range. law skips `branches` (and `acceptance`, `pilot`,
        # `tolerance`) here unless they are passed explicitly.
        reqs = {
            "voms": CreateVomsProxy.req(self),
            "cmssw": InstallCMSSW.req_different_branching(self, workflow="local"),
            "gridpack": MakeGridpack.req_different_branching(self),
        }
        premix = PremixFileList.req_different_branching(self, workflow="local")
        if premix.get_branch_map():
            reqs["premix"] = premix
        return reqs

    def requires(self):
        era, pi, _ = self.branch_data
        # MakeGridpack branches over distinct gridpacks, so map this point to its gridpack branch
        gp_branch = self.gridpack_index()[
            self.process.gridpack_name(self.prod_points[pi])
        ]
        reqs = {
            "voms": CreateVomsProxy.req(self),
            "cmssw": InstallCMSSW.req(
                self, branch=self.prod_eras.index(era), workflow="local"
            ),
            "gridpack": MakeGridpack.req(self, branch=gp_branch),
        }
        premix = PremixFileList.req(self, workflow="local")
        eras_with_premix = list(premix.get_branch_map().values())
        if era in eras_with_premix:
            reqs["premix"] = PremixFileList.req(
                self, branch=eras_with_premix.index(era), workflow="local"
            )
        return reqs

    def output(self):
        era, pi, seed = self.branch_data
        point = self.prod_points[pi]
        return {
            v: self.produced_nano_target(era, point, v, seed)
            for v in self.nano_versions(era)
        }

    def run(self):
        # `NanoMergeTask` requires only the seeds of the groups it merges, so a merge job whose
        # seed is not produced yet would run this 7 h chain inside a 3 h merge slot, on the
        # merge's single core, and lose it on walltime. Same reasoning as `MakeGridpack`: if the
        # submitted task cannot be determined, allow it rather than block a legitimate run.
        era, pi, seed = self.branch_data
        family = self.get_task_family().rsplit(".", 1)[-1]
        submitted = submitted_task_family()
        if on_batch_node() and submitted not in (None, family):
            raise RuntimeError(
                f"seed {seed} of {self.process.point_name(self.prod_points[pi])} ({era}) is not "
                f"produced yet, and this job was submitted to run {submitted}, not {family}: a "
                f"{submitted} job must not generate a sample on its own slot. Submit {family} "
                "for that seed first -- run_tools/merge_status.py says which merge groups are "
                "ready -- or check that fs_default is readable from the worker, which is the "
                "other way its record can look missing."
            )
        point = self.prod_points[pi]
        fragment = self.process.gen_fragment(point, era)
        n_evt = point.events_per_job
        with contextlib.ExitStack() as stack:
            gridpack = stack.enter_context(
                self.input()["gridpack"].localize("r")
            ).abspath
            work_dir, is_tmp = self.law_job_home()
            premix = self.input().get("premix")
            premix_list = (
                stack.enter_context(premix.localize("r")).abspath if premix else None
            )
            try:
                miniaod = run_step.run_chain(
                    self.conditions,
                    era,
                    self.prod_setup.get("first_step"),
                    "MINIAOD",
                    seed,
                    n_evt,
                    work_dir,
                    gridpack=gridpack,
                    fragment_path=fragment,
                    n_threads=int(self.n_cpus),
                    pileup_filelist=premix_list,
                )
                for version in self.nano_versions(era):
                    nano_out = run_step.run_nano(
                        self.conditions,
                        era,
                        version,
                        seed,
                        n_evt,
                        work_dir,
                        miniaod,
                        n_threads=int(self.n_cpus),
                    )
                    with self.staged_nano_target(era, point, version, seed).localize(
                        "w"
                    ) as out_local:
                        shutil.copy(nano_out, out_local.abspath)
                    # only once the nano file is on storage, so a record never outlives a
                    # missing file
                    self._write_produced_record(era, point, version, seed, n_evt)
            finally:
                if is_tmp:
                    shutil.rmtree(work_dir, ignore_errors=True)


class NanoMergeTask(Task, HTCondorWorkflow, CrabWorkflow, law.LocalWorkflow):
    """Merge a group of per-seed nano files into one, then drop the staged inputs (FLAF-friendly)."""

    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 3.0)

    def create_branch_map(self):
        fpm = int(self.prod_setup.get("files_per_merge", 20))
        branches = {}
        bid = 0
        for era in self.prod_eras:
            for pi, point in enumerate(self.prod_points):
                seeds = list(range(1, point.n_jobs(era) + 1))
                for version in self.nano_versions(era):
                    for group, group_seeds in merge_groups(seeds, fpm):
                        branches[bid] = (era, pi, version, group, group_seeds)
                        bid += 1
        return branches

    def _runprod_index(self):
        return {
            bd: i
            for i, bd in enumerate(runprod_branches(self.prod_eras, self.prod_points))
        }

    def required_runprod_branches(self):
        """`RunProd` branch ids of every seed in this merge workflow's *effective* branch map.

        Built from `get_branch_map()`, which law has already reduced to a `--branches` selection,
        so narrowing the merge narrows what has to be generated for it.
        """
        branch_map = self.get_branch_map()
        index = self._runprod_index()
        required = sorted(
            {
                index[(era, pi, seed)]
                for era, pi, _, _, seeds in branch_map.values()
                for seed in seeds
            }
        )
        if branch_map and not required:
            # law reads an empty `branches` as *all* branches, so a union that came out empty
            # would silently restore the whole-generation-stage requirement this replaced
            raise RuntimeError(
                f"{len(branch_map)} merge groups selected but not one seed behind them: the "
                "`RunProd` branch numbering and this branch map disagree."
            )
        return required

    def workflow_requires(self):
        # Only the seeds these groups merge, never the whole generation stage: with
        # `RunProd.req(self)` no group could run until the last branch of the production had, and
        # 169 of the 192 groups of Run3_2023BPix were complete with none merged. `req` was also
        # wrong in the other direction -- it copies `branches`, so `--branches 5` on the merge
        # asked for *RunProd* branch 5 rather than for the 50 seeds of merge group 5.
        # `req_different_branching` skips `branches` unless it is given, which it is here.
        return {
            "runprod": RunProd.req_different_branching(
                self,
                branches=tuple(law.util.range_join(self.required_runprod_branches())),
            )
        }

    def requires(self):
        era, pi, _, _, seeds = self.branch_data
        idx = self._runprod_index()
        return {seed: RunProd.req(self, branch=idx[(era, pi, seed)]) for seed in seeds}

    def output(self):
        era, pi, version, group, _ = self.branch_data
        return self.merged_nano_target(era, self.prod_points[pi], version, group)

    def _check_contracted_inputs(self, version, seeds):
        """Refuse a group whose seeds were not all produced at the same job size.

        `events_per_job * files_per_merge` is a contract -- one merged file per 50 000 events, to
        match the HLepRare skims -- and the only check the merge otherwise makes is
        `n_out == sum(n_in)`, which a group of mixed sizes satisfies because it is self-consistent.
        A sample re-produced at a different `events_per_job` would therefore merge quietly into
        files of the wrong size. The `produced/` records carry the size each seed was asked for,
        so compare those. Deliberately the *requested* size and not the delivered count: a job
        does not always return every event it was asked for -- one Run3_2023 job returned 999 of
        its 1000, leaving that merged file at 49 999 -- and refusing a group over one event would
        strand it, since re-running the seed yields the same number again. `n_out == sum(n_in)`
        below is what guards the merge itself. `--test` produces a single short job on purpose and
        is exempt.
        """
        if self.test > 0:
            return
        wanted = int(self.prod_setup["events_per_job"])
        # the records `RunProd` declares for these seeds, not a second path built by hand
        records = self.input()
        sizes = {}
        for seed in seeds:
            target = records[seed][version]
            try:
                requested = int(target.load(formatter="json")["events_requested"])
            except Exception as exc:
                raise RuntimeError(
                    f"cannot read the produced record of seed {seed} at {target.path}, so the "
                    f"job size of this merge group cannot be checked: {exc}"
                )
            sizes.setdefault(requested, []).append(seed)
        odd = {size: s for size, s in sizes.items() if size != wanted}
        if odd:
            shown = "; ".join(
                f"seed {', '.join(map(str, s[:4]))}{' ...' if len(s) > 4 else ''}: {size}"
                for size, s in sorted(odd.items())
            )
            raise RuntimeError(
                f"this merge group mixes job sizes: the setup asks for {wanted} events per job, "
                f"but {shown} events. Merging it would produce a file that is not "
                f"{wanted * int(self.prod_setup.get('files_per_merge', 20))} events. Re-produce "
                "the odd seeds at the setup's size, or delete their `produced/` records."
            )

    def run(self):
        era, pi, version, _, seeds = self.branch_data
        vparams = run_step.resolve_step_params(
            self.conditions, era, "NANO", version=version
        )
        # the RunProd requirement provides the `produced/` records, not the files themselves
        point = self.prod_points[pi]
        staged = [self.staged_nano_target(era, point, version, seed) for seed in seeds]
        missing = [t for t in staged if not t.exists()]
        if missing:
            raise RuntimeError(
                f"{len(missing)} of {len(staged)} staged nano files of this merge group are "
                f"gone (first: {missing[0].uri()}), although their seeds are recorded as "
                "produced. Either this group was merged before and its merged file was "
                "removed -- delete the seeds' `produced/` records to regenerate them -- or "
                "the storage lost them."
            )
        self._check_contracted_inputs(version, seeds)
        work_dir, is_tmp = self.law_job_home()
        try:
            with contextlib.ExitStack() as stack:
                local_ins = [
                    stack.enter_context(t.localize("r")).abspath for t in staged
                ]
                with self.output().localize("w") as out_local:
                    run_step.hadd_nano(
                        run_step.merge_params(vparams),
                        out_local.abspath,
                        local_ins,
                        work_dir,
                    )
                    n_out = run_step.count_events(vparams, out_local.abspath, work_dir)
                    n_in = sum(
                        run_step.count_events(vparams, p, work_dir) for p in local_ins
                    )
                    if n_out != n_in:
                        raise RuntimeError(
                            f"nano merge entry mismatch: merged {n_out} != sum inputs {n_in}"
                        )
            # merged output uploaded and verified -> remove the staged per-seed inputs
            for t in staged:
                t.remove()
        finally:
            if is_tmp:
                shutil.rmtree(work_dir, ignore_errors=True)


class BackfillProducedRecords(Task, law.LocalWorkflow):
    """Write the `produced/` records of seeds that ran before those records existed.

    `RunProd` used to declare the staged nano file itself as its output, so a production whose
    files `NanoMergeTask` had already merged and deleted looked entirely unproduced, and a
    restart resubmitted every seed of the era. The records fix that going forward; this task
    reconstructs them for work that is already done, from what is on storage:

      * a merged file accounts for the whole group of seeds behind it -- the merge writes it only
        after checking that its entry count equals the sum of its inputs, and deletes the inputs
        only after that;
      * a staged nano file that is still there accounts for its own seed.

    Existing records are left alone, so this is safe to re-run; delete a branch's
    `backfill.done` flag to make it list storage again. One branch per (era, point, version) keeps
    the listings small and lets the branches run in parallel.
    """

    def create_branch_map(self):
        branches = {}
        bid = 0
        for era in self.prod_eras:
            for pi, _ in enumerate(self.prod_points):
                for version in self.nano_versions(era):
                    branches[bid] = (era, pi, version)
                    bid += 1
        return branches

    def output(self):
        era, pi, version = self.branch_data
        name = self.process.point_name(self.prod_points[pi])
        return self.storage_target(
            "produced", f"nanoAOD_{version}", era, name, "backfill.done"
        )

    upload_threads = luigi.IntParameter(
        default=16,
        significant=False,
        description="records to upload at once; each upload is a remote round trip, so this is "
        "latency-bound and worth raising on a slow endpoint",
    )

    @staticmethod
    def _names(dir_target):
        """File names in a remote directory, empty when it does not exist yet."""
        try:
            return set(dir_target.listdir())
        except Exception:
            return set()

    def run(self):
        era, pi, version = self.branch_data
        point = self.prod_points[pi]
        fpm = int(self.prod_setup.get("files_per_merge", 20))
        seeds = list(range(1, point.n_jobs(era) + 1))

        name = self.process.point_name(point)

        # Three listings instead of a remote stat per seed. An era has 8300 seeds per nano
        # version, and at one round trip each the stats alone ran for hours -- long enough that
        # the first real migration had to be finished out of band.
        have_records = self._names(
            self.produced_nano_target(era, point, version, 1).parent
        )
        have_staged = self._names(
            self.staged_nano_target(era, point, version, 1).parent
        )
        have_merged = self._names(
            self.merged_nano_target(era, point, version, 0).parent
        )

        merged_seeds = set()
        for group, group_seeds in merge_groups(seeds, fpm):
            if f"nano_{version}_{group}.root" in have_merged:
                merged_seeds.update(group_seeds)

        todo, skipped, staged_seen = [], 0, 0
        for seed in seeds:
            if f"nano_{version}_{seed}.json" in have_records:
                skipped += 1
                continue
            if seed in merged_seeds:
                pass
            elif f"nano_{version}_{seed}.root" in have_staged:
                staged_seen += 1
            else:
                continue
            todo.append(seed)

        # ... and the writes in parallel, for the same reason
        if todo:
            with ThreadPoolExecutor(max_workers=self.upload_threads) as pool:
                list(
                    pool.map(
                        lambda seed: self._write_produced_record(
                            era, point, version, seed, point.events_per_job
                        ),
                        todo,
                    )
                )
        written = len(todo)

        print(
            f"BackfillProducedRecords[{era}/{name}/{version}]: {written} records written "
            f"({len(merged_seeds)} seeds covered by merged files, {staged_seen} by a staged "
            f"file), {skipped} already present, {len(seeds)} seeds in total"
        )
        with self.output().localize("w") as out_local:
            with open(out_local.abspath, "w") as f:
                f.write(f"{written} written, {skipped} present, {len(seeds)} seeds\n")
