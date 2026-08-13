"""CRAB backend for DSProd law tasks (more grid resources than local HTCondor).

Built on ``law.contrib.cms.CrabWorkflow``, modelled on FLAF PR #299 but much simpler:
CRAB is only the batch backend — DSProd writes all products (gridpacks, nano) to EOS via
law WLCG targets, so CRAB's own stageout/log transfer is forced off. WLCG workers have no
AFS, so the DSProd code + genproductions_scripts are shipped as a CRAB ``inputFiles`` tarball
(built at submit time) and unpacked by ``bootstrap.sh``; CMSSW is set up on the worker from
cvmfs on demand (our releases are standard central releases).

Config (in the prod_setup YAML, ``crab:`` section)::

    crab:
      storage_site: T3_CH_CERNBOX            # Site.storageSite (submit-time write-check only)
      out_lfn_base: /store/user/<you>/DSProd  # Data.outLFNDirBase (write-check only)
      # optional: whitelist: [T2_CH_CERN], blacklist: [...], max_memory_mb: 4000, max_cores: 4
"""

import math
import os
import re
import subprocess
import uuid

import law
import luigi
from law.job.base import JobInputFile

law.contrib.load("cms")


def build_code_tarball(ana_path, out_path):
    """Tar the DSProd code needed on a WLCG worker (no AFS there)."""
    includes = [
        "dsprod",
        "config",
        "env.sh",
        "bootstrap.sh",
        "genproductions_scripts",
        # vendored pure-python law + luigi (+ deps), used by env.sh on grid workers where
        # there is no PyPI access and the system python is too old to pip-install luigi.
        "soft/vendor",
    ]
    present = [p for p in includes if os.path.exists(os.path.join(ana_path, p))]
    subprocess.run(
        ["tar", "-czf", out_path, "--exclude=__pycache__", "--exclude=.git", *present],
        cwd=ana_path,
        check=True,
    )
    return out_path


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


_CrabProxyBase = law.cms.CrabWorkflow.workflow_proxy_cls


class DSProdCrabWorkflowProxy(_CrabProxyBase):
    def setup_job_manager(self):
        """Gate submission on a valid VOMS proxy + a MyProxy credential the CRAB server needs."""
        proxy = os.environ.get("X509_USER_PROXY", "")
        if not proxy or not os.path.isfile(proxy):
            raise RuntimeError(
                "CRAB needs a VOMS proxy (X509_USER_PROXY). Run: "
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
    """CRAB remote workflow mixin for DSProd tasks."""

    workflow_proxy_cls = DSProdCrabWorkflowProxy
    poll_interval = luigi.FloatParameter(default=5.0, significant=False)

    crab_memory = luigi.IntParameter(
        default=-1,
        significant=False,
        description="max memory per CRAB job in MB; -1 = auto",
    )
    crab_whitelist = law.CSVParameter(
        default=(),
        significant=False,
        description="CRAB Site.whitelist (empty = storage site)",
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
        return self.prod_setup.get("crab") or {}

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
        """Build (once per submission) the code tarball shipped via CRAB inputFiles."""
        out = os.path.join(self.local_path(), "dsprod_code.tar.gz")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        return build_code_tarball(self.ana_path(), out)

    def crab_stageout_location(self):
        cfg = self._crab_cfg()
        site = cfg.get("storage_site")
        lfn = cfg.get("out_lfn_base")
        if not site or not lfn:
            raise RuntimeError(
                "CRAB needs crab.storage_site and crab.out_lfn_base in the prod_setup "
                "(submit-time write-check only; DSProd products go to the `storage:` EOS path). "
                "Example: crab: {storage_site: T3_CH_CERNBOX, out_lfn_base: /store/user/$USER/DSProd}"
            )
        return str(site), str(lfn)

    def crab_output_directory(self):
        return law.LocalDirectoryTarget(self.local_path())

    def crab_request_name(self, submit_jobs):
        name = "_".join([self.task_family.replace(".", "_"), uuid.uuid4().hex[:8]])
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

    def crab_job_file_factory_cls(self):
        return DSProdCrabJobFileFactory

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
        # DSProd generation jobs have no input dataset, so they can run at any CMS processing
        # site. Do NOT auto-whitelist the *storage* site: it may not be a processing site
        # (e.g. T3_CH_CERNBOX) and CRAB then refuses the task ("not in the list of known CMS
        # Processing Site Names"). Restrict processing sites only when explicitly configured.
        if whitelist:
            config.crab.Site.whitelist = [str(s) for s in whitelist]
            config.crab.Site.ignoreGlobalBlacklist = True
        if blacklist:
            config.crab.Site.blacklist = [str(s) for s in blacklist]
        config.crab.Data.ignoreLocality = True
        return config
