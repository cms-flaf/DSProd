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
import math
import os
import re
import shutil
import subprocess
import tempfile
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
    ps_call,
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
        else:
            p = run_step.resolve_step_params(conditions, era, step)
            releases.add((p["SCRAM_ARCH"], p["CMSSW"]))
    return sorted(releases)


class Task(law.Task):
    setup = luigi.Parameter(description="path to the production setup YAML")
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
        if setup_path != Task.setup_path:
            raise RuntimeError(
                f"Inconsistent setup path: {setup_path} != {Task.setup_path}"
            )
        self.prod_setup = Task.prod_setup
        self.conditions = Task.conditions
        self.process = Task.process
        self.prod_points = self._select_points(Task.all_points)
        _, setup_full_name = os.path.split(setup_path)
        self.setup_name, _ = os.path.splitext(setup_full_name)

    def _select_points(self, points):
        """Apply `--points` and `--test`.

        Selecting a subset only narrows what this run produces: output paths are keyed by era,
        point name and seed, never by branch id, so a selective run writes exactly where the full
        production would.
        """
        if self.points:
            patterns = list(self.points)
            points = [
                p
                for p in points
                if any(fnmatch.fnmatch(p.name, pat) for pat in patterns)
            ]
            if not points:
                raise RuntimeError(
                    f"--points {','.join(patterns)} matches no point of {self.setup}"
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
    @property
    def eras(self):
        return self.prod_setup["eras"]

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
        if self.points:
            patterns = ",".join(sorted(self.points))
            slug = re.sub(r"\W+", "", "".join(sorted(self.points)))[:24]
            name += f"_{slug}{hashlib.sha1(patterns.encode()).hexdigest()[:6]}"
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
        if "LAW_JOB_HOME" in os.environ:
            return os.environ["LAW_JOB_HOME"], False
        os.makedirs(self.local_path(), exist_ok=True)
        return tempfile.mkdtemp(dir=self.local_path()), True


class HTCondorWorkflowProxy(law.htcondor.workflow.HTCondorWorkflowProxy):
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
        return dict(enumerate(self.eras))

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
    the decay channels of X->HH->bbWW, where the Higgses leave the generator undecayed), and one
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


class RunProd(Task, HTCondorWorkflow, CrabWorkflow, law.LocalWorkflow):
    """Fused GEN->NANO production for one (era, point, seed); stages one nano per version."""

    max_runtime = copy_param(HTCondorWorkflow.max_runtime, 24.0)
    n_cpus = copy_param(HTCondorWorkflow.n_cpus, 4)

    def create_branch_map(self):
        return dict(enumerate(runprod_branches(self.eras, self.prod_points)))

    def workflow_requires(self):
        return {
            "voms": CreateVomsProxy.req(self),
            "cmssw": InstallCMSSW.req(self, workflow="local"),
            "gridpack": MakeGridpack.req(self),
        }

    def requires(self):
        era, pi, _ = self.branch_data
        # MakeGridpack branches over distinct gridpacks, so map this point to its gridpack branch
        gp_branch = self.gridpack_index()[
            self.process.gridpack_name(self.prod_points[pi])
        ]
        return {
            "voms": CreateVomsProxy.req(self),
            "cmssw": InstallCMSSW.req(
                self, branch=self.eras.index(era), workflow="local"
            ),
            "gridpack": MakeGridpack.req(self, branch=gp_branch),
        }

    def _staged_target(self, era, point, version, seed):
        name = self.process.point_name(point)
        return self.storage_target(
            "staging", f"nanoAOD_{version}", era, name, f"nano_{version}_{seed}.root"
        )

    def output(self):
        era, pi, seed = self.branch_data
        point = self.prod_points[pi]
        return {
            v: self._staged_target(era, point, v, seed) for v in self.nano_versions(era)
        }

    def run(self):
        era, pi, seed = self.branch_data
        point = self.prod_points[pi]
        fragment = self.process.gen_fragment(point, era)
        n_evt = point.events_per_job
        with contextlib.ExitStack() as stack:
            gridpack = stack.enter_context(
                self.input()["gridpack"].localize("r")
            ).abspath
            work_dir, is_tmp = self.law_job_home()
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
                )
                for version in self.nano_versions(era):
                    nano_out = run_step.run_nano(
                        self.conditions, era, version, seed, n_evt, work_dir, miniaod
                    )
                    with self.output()[version].localize("w") as out_local:
                        shutil.copy(nano_out, out_local.abspath)
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
        for era in self.eras:
            for pi, point in enumerate(self.prod_points):
                seeds = list(range(1, point.n_jobs(era) + 1))
                for version in self.nano_versions(era):
                    for group, start in enumerate(range(0, len(seeds), fpm)):
                        branches[bid] = (
                            era,
                            pi,
                            version,
                            group,
                            seeds[start : start + fpm],
                        )
                        bid += 1
        return branches

    def _runprod_index(self):
        return {
            bd: i for i, bd in enumerate(runprod_branches(self.eras, self.prod_points))
        }

    def workflow_requires(self):
        return {"runprod": RunProd.req(self)}

    def requires(self):
        era, pi, _, _, seeds = self.branch_data
        idx = self._runprod_index()
        return {seed: RunProd.req(self, branch=idx[(era, pi, seed)]) for seed in seeds}

    def output(self):
        era, pi, version, group, _ = self.branch_data
        name = self.process.point_name(self.prod_points[pi])
        return self.storage_target(
            f"nanoAOD_{version}", era, name, f"nano_{version}_{group}.root"
        )

    def run(self):
        era, pi, version, _, seeds = self.branch_data
        vparams = run_step.resolve_step_params(
            self.conditions, era, "NANO", version=version
        )
        staged = [self.input()[seed][version] for seed in seeds]
        work_dir, is_tmp = self.law_job_home()
        try:
            with contextlib.ExitStack() as stack:
                local_ins = [
                    stack.enter_context(t.localize("r")).abspath for t in staged
                ]
                with self.output().localize("w") as out_local:
                    run_step.hadd_nano(vparams, out_local.abspath, local_ins, work_dir)
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
