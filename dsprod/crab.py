"""CRAB backend for DSProd law tasks (more grid resources than local HTCondor).

Built on ``law.contrib.cms.CrabWorkflow``, modelled on FLAF PR #299 but much simpler:
CRAB is only the batch backend — DSProd writes all products (gridpacks, nano) to EOS via
law WLCG targets, so CRAB's own stageout/log transfer is forced off. WLCG workers have no
AFS, so the DSProd code + genproductions_scripts are shipped as a CRAB ``inputFiles`` tarball
(built at submit time) and unpacked by ``bootstrap.sh``; CMSSW is set up on the worker from
cvmfs on demand (our releases are standard central releases).

There is **no CRAB-specific output location**: products always go to ``fs_default``, whatever the
backend. Only the compute knobs are configurable, in the merged global config
(``config/global.yaml`` + ``user_custom.yaml``), never in a production setup::

    crab:
      max_memory_mb: 2500
      max_cores: 1
      # whitelist: [ ... ]       # optional; default = every tier (T1_*, T2_*, T3_*)
      # blacklist: [ ... ]       # optional; exclude sites that fail to reach the storage
      # parallel_jobs: 5000      # jobs per CRAB task / in flight; --parallel-jobs wins
      # refill_fraction: 0.2     # min wave size / free slots, as a fraction of parallel_jobs
"""

import fnmatch
import math
import os
import re
import subprocess
import uuid
from collections import Counter, OrderedDict

import law
import luigi
from law.job.base import JobInputFile

from .site_stats import SiteStats
from .tools import (
    ResyncExistingBranchesProxy,
    timed_call_wrapper,
    update_kerberos_ticket,
)

law.contrib.load("cms")

#: site CRAB is told to stage out to. Never actually written to (stageout is disabled), but the
#: submit-time check requires a site the user can write to; CERNBOX is the CERN-account default.
_CRAB_DUMMY_SITE = "T3_CH_CERNBOX"

#: Site.whitelist used when the config sets none. It is NOT optional: DSProd jobs have no input
#: dataset, so the config sets `Data.ignoreLocality`, and the CRAB client then refuses to submit
#: without a whitelist ("when ignoreLocality is set a valid site white list must be specified",
#: CRABClient/Commands/submit.py). Listing every tier is the widest pool the client accepts.
_CRAB_ALL_SITES = ("T1_*", "T2_*", "T3_*")

#: jobs per CRAB task, and the number law keeps in flight. A CRAB task tops out around 10k jobs,
#: so a large production (tens of thousands of branches) must be split into waves rather than
#: submitted as one task.
_CRAB_DEFAULT_PARALLEL_JOBS = 5000

#: minimum size of a wave, as a fraction of `parallel_jobs`, and the number of slots that must be
#: free to take it. Without it law submits a fresh CRAB task as soon as a single job finishes or
#: fails, producing hundreds of tiny tasks.
_CRAB_DEFAULT_REFILL_FRACTION = 0.2


def build_code_tarball(ana_path, out_path):
    """Tar the DSProd code needed on a WLCG worker (no AFS there).

    Deliberately **without** `gridpacks`: a CRAB input sandbox is size-limited, and a job that
    needs a gridpack downloads it from `fs_default` (where `MakeGridpack` put it) at run time.
    """
    includes = [
        "dsprod",
        "models",  # model plugins + cards + fragments (DSProdModels submodule)
        "config",
        "env.sh",
        "bootstrap.sh",
        "genproductions_scripts",
        # vendored pure-python law + luigi (+ deps), used by env.sh on grid workers where
        # there is no PyPI access and the system python is too old to pip-install luigi.
        "soft/vendor",
    ]
    present = [p for p in includes if os.path.exists(os.path.join(ana_path, p))]

    # Build next to the destination and rename, so a failed build never leaves a truncated
    # tarball where the next submission (or `bootstrap.sh`, which globs `dsprod_code*.tar.gz`)
    # would pick it up.
    tmp_path = f"{out_path}.tmp"
    proc = subprocess.run(
        [
            "tar",
            "-czf",
            tmp_path,
            "--warning=no-file-changed",
            "--exclude=__pycache__",
            "--exclude=.git",
            *present,
        ],
        cwd=ana_path,
        capture_output=True,
        text=True,
    )
    # GNU tar exits 1 when a file or directory changed while it was being read -- routine on an
    # EOS-mounted production area, and harmless: the entry is still archived in full. Failing here
    # aborts the submission of a whole production, so verify the archive instead of trusting the
    # exit code. 2 and above are real errors.
    if proc.returncode >= 2:
        _remove_quietly(tmp_path)
        raise RuntimeError(
            f"could not build the CRAB code tarball (tar exit {proc.returncode}):\n"
            f"{proc.stderr.strip()}"
        )
    try:
        _verify_code_tarball(tmp_path, present)
    except Exception:
        _remove_quietly(tmp_path)
        raise
    os.replace(tmp_path, out_path)
    return out_path


def _remove_quietly(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _verify_code_tarball(path, expected):
    """Raise unless the archive is readable and holds every requested top-level entry."""
    proc = subprocess.run(["tar", "-tzf", path], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"the CRAB code tarball {path} is not readable:\n{proc.stderr.strip()}"
        )
    top = {name.split("/", 1)[0] for name in proc.stdout.split("\n") if name}
    missing = [e for e in expected if e.split("/", 1)[0] not in top]
    if missing:
        raise RuntimeError(
            f"the CRAB code tarball {path} is incomplete, missing: {', '.join(missing)}"
        )


class DSProdCrabJobFileFactory(law.cms.CrabJobFileFactory):
    """CRAB job file with no CRAB-side product/log transfer (DSProd owns remote I/O)."""

    def create(self, **kwargs):
        kwargs = dict(kwargs)
        kwargs["output_files"] = []
        job_file, c = super().create(**kwargs)
        if hasattr(c, "crab"):
            c.crab.General.transferOutputs = False
            c.crab.General.transferLogs = False
            if getattr(c.crab, "JobType", None) is not None:
                c.crab.JobType.sendPythonFolder = None
                c.crab.JobType.outputFiles = None
                c.crab.JobType.disableAutomaticOutputCollection = True
        c.output_files = []
        # the config-object tweaks above are not reflected in the already-written crab cfg, so
        # rewrite it: strip the deprecated sendPythonFolder (rejected by modern CRAB) and force
        # no CRAB-side transfers (DSProd owns remote I/O).
        try:
            self._rewrite_crab_job_file(job_file)
        except Exception as exc:
            print(f"WARNING: could not post-process crab job file {job_file}: {exc}")
        return job_file, c

    @staticmethod
    def _rewrite_crab_job_file(job_file):
        with open(job_file) as f:
            lines = f.readlines()
        new_lines = []
        skip_list = False
        for ln in lines:
            stripped = ln.strip()
            if "sendPythonFolder" in ln:
                continue
            if "General.transferOutputs" in ln:
                new_lines.append("cfg.General.transferOutputs = False\n")
                continue
            if "General.transferLogs" in ln:
                new_lines.append("cfg.General.transferLogs = False\n")
                continue
            if "JobType.outputFiles" in ln:
                if stripped.endswith("[") or ("[" in stripped and "]" not in stripped):
                    skip_list = True
                continue
            if skip_list:
                if "]" in stripped:
                    skip_list = False
                continue
            new_lines.append(ln)
        with open(job_file, "w") as f:
            f.writelines(new_lines)


#: CMS site names, from cvmfs — no network, and present wherever CRAB runs
_SITECONF = "/cvmfs/cms.cern.ch/SITECONF"


def known_sites():
    """Every CMS site name cvmfs knows about."""
    try:
        return sorted(
            n for n in os.listdir(_SITECONF) if re.match(r"^T\d_[A-Za-z0-9_]+$", n)
        )
    except OSError:
        return []


def resolve_whitelist(whitelist, blacklist):
    """A `Site.whitelist` from which `blacklist` is actually absent.

    CRAB gives the whitelist precedence: a site matched by both is *kept*, and it says so only in a
    warning ("Since the whitelist has precedence, these sites are not considered in the blacklist").
    With the default all-tier globs that silently defeats every exclusion -- the configured
    `crab.blacklist` and the automatic site quarantine alike.

    So a tier glob covering an excluded site is expanded, from the cvmfs site list, into the sites
    it actually matches minus the excluded ones. Globs covering nothing excluded are left alone,
    which keeps the pool wide and the expansion small: excluding one T2 lists the T2s and leaves
    `T1_*` and `T3_*` as they are. The storage site is dropped as well -- it need not be a
    processing site, and CRAB refuses a task whose whitelist names one.
    """
    if not blacklist:
        return list(whitelist)
    sites = known_sites()
    out = []
    for entry in whitelist:
        hit = [b for b in blacklist if fnmatch.fnmatch(b, entry)]
        if not hit:
            out.append(entry)
            continue
        if not sites:
            raise RuntimeError(
                f"cannot exclude {', '.join(hit)}: they are covered by the whitelist entry "
                f"'{entry}', and CRAB keeps a site that appears in both. Expanding it needs the "
                f"site list at {_SITECONF}, which is not readable. Set `crab.whitelist` explicitly."
            )
        out += [
            site
            for site in sites
            if fnmatch.fnmatch(site, entry)
            and site not in blacklist
            and site != _CRAB_DUMMY_SITE
        ]
    return out


_CrabProxyBase = law.cms.CrabWorkflow.workflow_proxy_cls


def _cli_has_parallel_jobs():
    """True when the user passed ``--parallel-jobs`` (or a task-prefixed form)."""
    parser = luigi.cmdline_parser.CmdlineParser.get_instance()
    tokens = list(getattr(parser, "cmdline_args", None) or [])
    for tok in tokens:
        if tok in ("--parallel-jobs", "--parallel_jobs"):
            return True
        if tok.startswith("--parallel-jobs=") or tok.startswith("--parallel_jobs="):
            return True
        if tok.endswith("-parallel-jobs") or tok.endswith("-parallel_jobs"):
            return True
        if "-parallel-jobs=" in tok or "-parallel_jobs=" in tok:
            return True
    return False


class DSProdCrabWorkflowProxy(ResyncExistingBranchesProxy, _CrabProxyBase):
    def __init__(self, *args, **kwargs):
        super(DSProdCrabWorkflowProxy, self).__init__(*args, **kwargs)
        self._apply_crab_parallel_jobs()

    def _apply_crab_parallel_jobs(self):
        """Cap the jobs in flight, and therefore the size of one CRAB task.

        law's default is unlimited, and DSProd tasks also inherit `HTCondorWorkflow`, so the
        value can only be fixed here: a production with tens of thousands of branches would
        otherwise be submitted as a single CRAB task, far above what a task can hold.
        """
        if _cli_has_parallel_jobs():
            return
        cfg_n = self.task._crab_cfg().get("parallel_jobs")
        if cfg_n is not None:
            self._set_parallel_jobs(int(cfg_n))
            return
        if self.poll_data.n_parallel == self.n_parallel_max:
            self._set_parallel_jobs(_CRAB_DEFAULT_PARALLEL_JOBS)

    def _crab_refill_fraction(self):
        raw = self.task._crab_cfg().get(
            "refill_fraction", _CRAB_DEFAULT_REFILL_FRACTION
        )
        try:
            frac = float(raw)
        except (TypeError, ValueError):
            frac = _CRAB_DEFAULT_REFILL_FRACTION
        return min(max(frac, 0.0), 1.0)

    def _should_submit_crab_group(self, n_waiting):
        """Whether to submit now, or hold the jobs back so they accumulate into one CRAB task.

        Creating a CRAB task is expensive and a task holds only a few thousand jobs, so a
        production is submitted in waves of at least `refill_fraction * parallel_jobs` jobs. Jobs
        are held back only while such a wave is still **achievable**: once the work left in the
        whole production -- running plus waiting -- can no longer fill one, waiting can only delay
        it, so whatever is waiting goes out immediately, however little that is. That covers the
        tail of a large production and every small production (which can never fill a wave and so
        is never batched at all), while a trickle of retries early on still accumulates.

        `n_waiting` (unsubmitted + jobs offered for retry) is what makes this an aggregation
        threshold at all. Gating on free slots alone let a handful of retries out as their own CRAB
        task whenever the production did not fill `parallel_jobs`: with 3270 of 5000 slots taken,
        1730 were free, so the gate was open from the first poll onwards.
        """
        n_parallel = self.poll_data.n_parallel
        if n_parallel >= self.n_parallel_max:
            # unlimited parallelism: keep law's own behaviour
            return True
        if n_waiting <= 0:
            return True
        n_active = self.poll_data.n_active
        min_wave = self._crab_refill_fraction() * n_parallel
        # a full-sized wave, and the room to run it
        if min(n_waiting, n_parallel - n_active) >= min_wave:
            return True
        # even if every job still running were to fail, the next wave could not reach the bar
        return n_active + n_waiting < min_wave

    def submit(self, retry_jobs=None):
        retry_jobs = retry_jobs or OrderedDict()
        n_waiting = len(self.job_data.unsubmitted_jobs) + len(retry_jobs)
        if self._should_submit_crab_group(n_waiting):
            return super(DSProdCrabWorkflowProxy, self).submit(retry_jobs or None)

        # park retries as unsubmitted, so the next eligible wave picks them up as one larger
        # CRAB task instead of creating a task for a handful of jobs now
        if retry_jobs:
            for job_num, branches in retry_jobs.items():
                if self._can_skip_job(job_num, branches):
                    continue
                self.job_data.jobs.pop(job_num, None)
                self.job_data.unsubmitted_jobs[job_num] = branches
            self.dump_job_data()
        return OrderedDict()

    def setup_job_manager(self):
        """Gate submission on a valid VOMS proxy + a MyProxy credential the CRAB server needs."""
        proxy = os.environ.get("X509_USER_PROXY", "")
        if not proxy or not os.path.isfile(proxy):
            raise RuntimeError(
                "CRAB needs a VOMS proxy (X509_USER_PROXY). Run: "
                "voms-proxy-init --voms cms -valid 192:00"
            )
        if not law.wlcg.check_vomsproxy_validity(proxy_file=proxy):
            raise RuntimeError(
                f"VOMS proxy at {proxy} is expired. Run: "
                "voms-proxy-init --voms cms -valid 192:00"
            )
        kwargs = {"proxy": proxy}
        min_myproxy_seconds = 5 * 24 * 3600
        for encode in (False, True):
            try:
                info = (
                    law.wlcg.get_myproxy_info(encode_username=encode, silent=True) or {}
                )
            except Exception:
                info = {}
            if info.get("username") and info.get("timeleft", 0) >= min_myproxy_seconds:
                kwargs["myproxy_username"] = info["username"]
                return kwargs
        raise RuntimeError(
            "CRAB requires a MyProxy credential valid for >= 5 days (the CRAB server "
            "retrieves it from myproxy.cern.ch). Run once:\n"
            "  myproxy-init -d -n -s myproxy.cern.ch\n"
            "  # verify: myproxy-info -d -s myproxy.cern.ch  (timeleft >= 5 days)"
        )


class CrabWorkflow(law.cms.CrabWorkflow):
    """CRAB remote workflow mixin for DSProd tasks.

    A production of tens of thousands of branches is submitted as a series of CRAB tasks of
    `crab.parallel_jobs` jobs each (see `DSProdCrabWorkflowProxy`), so no manual chunking of the
    branch range is needed.
    """

    workflow_proxy_cls = DSProdCrabWorkflowProxy
    poll_interval = luigi.FloatParameter(default=5.0, significant=False)

    #: lazily-built, throttled `kinit -R` used while polling (see crab_poll_callback)
    _crab_kerberos_update = None

    #: code tarball shipped to the workers, built once per law process (see _code_tarball)
    _code_tarball_path = None

    #: rolling per-site job statistics, and the (job_num, attempt, ok) keys already counted
    _site_stats_obj = None
    _site_stats_seen = None

    crab_memory = luigi.IntParameter(
        default=-1,
        significant=False,
        description="max memory per CRAB job in MB; -1 = auto",
    )
    crab_whitelist = law.CSVParameter(
        default=(),
        significant=False,
        description="CRAB Site.whitelist; empty (default) = all CMS processing sites",
    )
    crab_blacklist = law.CSVParameter(default=(), significant=False)

    exclude_params_branch = getattr(
        law.cms.CrabWorkflow, "exclude_params_branch", set()
    ) | {
        "crab_memory",
        "crab_whitelist",
        "crab_blacklist",
    }

    def _crab_cfg(self):
        """CRAB site/resource settings from the merged global config (`config/global.yaml` +
        `user_custom.yaml`), NOT the production setup — so a setup is backend-agnostic and
        identical for htcondor and crab."""
        from .config import get_global

        return get_global().get("crab", {}) or {}

    def _ensure_crab_pset(self, n_threads):
        """Minimal PSet whose numberOfThreads matches JobType.numCores (CRAB requires it)."""
        n_threads = max(1, int(n_threads))
        out_dir = self.local_path()
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"crab_PSet_threads{n_threads}.py")
        content = (
            "import FWCore.ParameterSet.Config as cms\n"
            'process = cms.Process("LAW")\n'
            'process.source = cms.Source("PoolSource", fileNames=cms.untracked.vstring([""]))\n'
            "process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(1))\n"
            "process.options = cms.untracked.PSet(\n"
            f"    numberOfThreads=cms.untracked.uint32({n_threads}),\n"
            "    numberOfStreams=cms.untracked.uint32(0),\n"
            ")\n"
        )
        if (not os.path.exists(path)) or open(path).read() != content:
            with open(path, "w") as f:
                f.write(content)
        return path

    def _code_tarball(self):
        """The code tarball shipped via CRAB inputFiles, built once per law process.

        A large production is submitted in several waves; rebuilding per wave would ship
        different code to different jobs if the checkout is touched meanwhile.
        """
        if self._code_tarball_path is None:
            out = os.path.join(self.local_path(), "dsprod_code.tar.gz")
            os.makedirs(os.path.dirname(out), exist_ok=True)
            self._code_tarball_path = build_code_tarball(self.ana_path(), out)
        return self._code_tarball_path

    def crab_stageout_location(self):
        """CRAB demands a `Site.storageSite` + `Data.outLFNDirBase` even when it transfers nothing.
        DSProd disables CRAB stageout and writes every product to `fs_default` (the same location
        as any other backend), so these are a submit-time formality and are filled in here rather
        than configured — there is deliberately no separate CRAB output location.
        """
        user = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
        return _CRAB_DUMMY_SITE, f"/store/user/{user}/DSProd_crab_unused"

    def crab_output_directory(self):
        return law.LocalDirectoryTarget(self.local_path())

    def crab_request_name(self, submit_jobs):
        # a large production is submitted as many CRAB tasks; naming them after the setup keeps
        # them identifiable in `crab status` and the monitoring dashboard
        name = "_".join(
            [
                self.task_family.replace(".", "_"),
                str(self.setup_name).replace(".", "_"),
                uuid.uuid4().hex[:8],
            ]
        )
        return re.sub(r"[^A-Za-z0-9_\-]", "_", name)[:100]

    def crab_bootstrap_file(self):
        from law.job.base import JobInputFile

        return JobInputFile(
            path=os.path.join(self.ana_path(), "bootstrap.sh"),
            copy=True,
            share=True,
            render_job=True,
        )

    def crab_workflow_requires(self):
        return {}

    def crab_check_job_completeness(self):
        return False

    def crab_poll_callback(self, poll_data):
        # a large CRAB production polls for days, while law keeps writing its job status files to
        # the AFS work area — renew the Kerberos ticket as the HTCondor backend does
        if self._crab_kerberos_update is None:

            def renew_kerberos_ticket():
                # verbose: a silent renewal leaves no way to tell, after a credential failure,
                # whether it had been running at all
                update_kerberos_ticket(verbose=1)

            krenew = float(getattr(self, "krenew", 1) or 0)
            self._crab_kerberos_update = (
                timed_call_wrapper(renew_kerberos_ticket, krenew * 3600)
                if krenew > 0
                else (lambda: None)
            )
        self._crab_kerberos_update()
        self._collect_site_stats()
        return True

    def site_stats(self):
        """Rolling per-site job record, kept in the production area across runs."""
        if self._site_stats_obj is None:
            self._site_stats_obj = SiteStats(
                os.path.join(self.ana_data_path(), "crab_site_stats.json"),
                self._crab_cfg().get("auto_blacklist"),
            )
            self._site_stats_seen = set()
        return self._site_stats_obj

    def _collect_site_stats(self):
        """Record the outcome of every job that reached a terminal state since the last poll.

        CRAB reports where a job ran in `SiteHistory`, which law stores as `extra.site_history`.
        Jobs still in flight are counted too -- not as outcomes, but as part of what was sent to a
        site, which is the denominator its failure rate is measured against.
        Each attempt is counted once: `job_data.attempts` has already been incremented by the time
        a job shows up as RETRY, and the ok flag separates a retry that then succeeded from the
        failure that preceded it.
        """
        proxy = self.workflow_proxy
        manager = proxy.job_manager
        terminal = (manager.FINISHED, manager.FAILED, manager.RETRY)
        stats = self.site_stats()
        in_flight = Counter()
        for job_num, data in proxy.job_data.jobs.items():
            status = data.get("status")
            history = (data.get("extra") or {}).get("site_history") or []
            if not history:
                continue
            if status not in terminal:
                # still pending or running: part of what was sent to the site, but no outcome yet
                in_flight[history[-1]] += 1
                continue
            ok = status == manager.FINISHED
            key = (job_num, proxy.job_data.attempts.get(job_num, 0), ok)
            if key in self._site_stats_seen:
                continue
            self._site_stats_seen.add(key)
            stats.record(history[-1], ok)
        stats.set_in_flight(in_flight)
        stats.save()

    def crab_job_file_factory_cls(self):
        return DSProdCrabJobFileFactory

    def crab_create_job_manager(self, **kwargs):
        """Create the job manager, and build its CMSSW sandbox, before anything is submitted.

        law builds that sandbox lazily, inside every submission attempt. A failure there is
        swallowed per job: each one is stored with `dummy_job_id`, polled as "unknown job id",
        retried, and the workflow only dies when the retry tolerance is exceeded -- half an hour
        later, with the real cause nowhere in the log. Building it here turns that into a single
        actionable error before the first submission.
        """
        manager = super().crab_create_job_manager(**kwargs)
        try:
            manager.cmssw_env
        except Exception as exc:
            raise RuntimeError(
                "could not set up the CMSSW sandbox that law runs `crab` in: "
                f"{exc}\nThis usually means `python` on PATH is not the DSProd shim (the "
                "sandbox dumps its environment with bare `python`, which CMSSW no longer "
                "ships). Source env.sh in this shell -- it writes soft/bin/python and prepends "
                "soft/bin to PATH -- and submit again."
            ) from exc
        return manager

    def crab_job_config(self, config, job_nums, branches=None):
        n_cpus = max(1, int(getattr(self, "n_cpus", 1) or 1))
        mem = int(self.crab_memory)
        if mem <= 0:
            mem = int(self._crab_cfg().get("max_memory_mb", n_cpus * 2500))
        mb_per_core = int(self._crab_cfg().get("max_memory_mb_per_core", 2500))
        max_cores = int(self._crab_cfg().get("max_cores", 8))
        n_cores = max(n_cpus, (mem + mb_per_core - 1) // mb_per_core)
        n_cores = max(1, min(n_cores, max_cores))
        mem = max(mem, n_cores * mb_per_core)
        # the CRAB client refuses a task above max(5000, 2500 * numCores) MB, so clamp instead of
        # letting a generous `max_memory_mb` (with a low `max_cores`) fail the whole submission
        mem = min(mem, max(5000, 2500 * n_cores))

        config.crab.JobType.psetName = self._ensure_crab_pset(n_cores)
        config.crab.JobType.numCores = n_cores
        config.crab.JobType.maxMemoryMB = mem

        # ship the DSProd code (no AFS on WLCG workers). This MUST go through law's
        # input_files dict: the job-file factory rebuilds JobType.inputFiles from it and
        # would overwrite any value written directly to config.crab.JobType.inputFiles.
        # law_job.sh symlinks every input file into LAW_JOB_HOME (the bootstrap's CWD), so
        # the tarball lands exactly where bootstrap.sh looks for it. postfix=False keeps the
        # name `dsprod_code.tar.gz` the bootstrap checks; render=False (binary tarball).
        config.input_files["dsprod_code"] = JobInputFile(
            self._code_tarball(),
            copy=True,
            share=True,
            postfix=False,
            render=False,
        )

        max_runtime = getattr(self, "max_runtime", None)
        if max_runtime is not None and float(max_runtime) > 0:
            floor = int(self._crab_cfg().get("min_runtime_min", 60))
            config.crab.JobType.maxJobRuntimeMin = max(
                int(math.floor(float(max_runtime) * 60)), floor
            )

        whitelist = list(self.crab_whitelist) or list(
            self._crab_cfg().get("whitelist") or []
        )
        blacklist = list(self.crab_blacklist) or list(
            self._crab_cfg().get("blacklist") or []
        )
        # DSProd generation jobs have no real input dataset, so they can run at ANY CMS processing
        # site — but `ignoreLocality` below makes a whitelist mandatory for the CRAB client, so an
        # unset one becomes every tier rather than nothing. Configuring one can only narrow the
        # pool. Do NOT auto-whitelist the *storage* site either: it may not be a processing site
        # (e.g. T3_CH_CERNBOX) and CRAB then refuses the task ("not in the list of known CMS
        # Processing Site Names").
        # sites quarantined by their recent failure record; every wave is a new CRAB task, so
        # this takes effect for the next one -- retries included
        quarantined = [s for s in self.site_stats().blacklist() if s not in blacklist]
        if quarantined:
            self.publish_message(
                "keeping {} site(s) out of this CRAB task after recent failures: {}".format(
                    len(quarantined), ", ".join(quarantined)
                )
            )
            blacklist = list(blacklist) + quarantined

        sites = resolve_whitelist(whitelist or _CRAB_ALL_SITES, blacklist)
        config.crab.Site.whitelist = [str(s) for s in sites]
        if blacklist:
            config.crab.Site.blacklist = [str(s) for s in blacklist]
        # Keep CMS's global blacklist of known-broken sites in force unless explicitly waived:
        # with an open site pool it is the main protection against burning jobs at bad sites.
        if self._crab_cfg().get("ignore_global_blacklist", False):
            config.crab.Site.ignoreGlobalBlacklist = True
        config.crab.Data.ignoreLocality = True
        return config
