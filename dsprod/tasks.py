"""DSProd law task framework: base Task + HTCondor workflow.

Phase 1 provides the generic scaffolding shared by every production task:
  * `Task` — loads the production setup (points via the process module) and the per-era
    conditions, and provides storage/target helpers.
  * `HTCondorWorkflow` (+ kerberos-renewing proxy) — long CMSSW jobs on HTCondor.
The concrete production tasks (MakeGridpack, RunProd, NanoMergeTask, MakeManifest) are
added in later phases and reuse these bases.
"""

import copy
import math
import os
import tempfile

import law
import luigi
import yaml

from . import registry
from .tools import timed_call_wrapper, update_kerberos_ticket

law.contrib.load("htcondor")
law.contrib.load("wlcg")

#: path prefixes that must be served by a remote (WLCG/gfal) target
_REMOTE_PREFIXES = ("davs://", "root://", "gsiftp://", "/eos/")


def copy_param(ref_param, new_default):
    param = copy.deepcopy(ref_param)
    param._default = new_default
    return param


def is_remote_path(path):
    return path.startswith(_REMOTE_PREFIXES)


class Task(law.Task):
    setup = luigi.Parameter(description="path to the production setup YAML")

    # class-level cache: a single setup is loaded once per process
    setup_path = None
    prod_setup = None
    conditions = None
    process = None
    points = None

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
            Task.points = Task.process.enumerate_points(Task.prod_setup)
            Task.setup_path = setup_path
        if setup_path != Task.setup_path:
            raise RuntimeError(
                f"Inconsistent setup path: {setup_path} != {Task.setup_path}"
            )
        self.prod_setup = Task.prod_setup
        self.conditions = Task.conditions
        self.process = Task.process
        self.points = Task.points
        _, setup_full_name = os.path.split(setup_path)
        self.setup_name, _ = os.path.splitext(setup_full_name)

    # ---- setup accessors ----------------------------------------------------
    @property
    def eras(self):
        return self.prod_setup["eras"]

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
        return (self.__class__.__name__, self.setup_name)

    def local_path(self, *path):
        parts = (self.ana_data_path(),) + self.store_parts() + path
        return os.path.join(*parts)

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))

    def target(self, path):
        """A remote (WLCG) or local file target, chosen by the path prefix."""
        if is_remote_path(path):
            return law.wlcg.WLCGFileTarget(path)
        return law.LocalFileTarget(path)

    def storage_path(self, *parts):
        return os.path.join(self.prod_setup["storage"], *parts)

    def storage_target(self, *parts):
        return self.target(self.storage_path(*parts))

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
