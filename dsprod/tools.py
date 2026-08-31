"""Minimal utilities vendored from FLAF/RunKit.

DSProd deliberately does not depend on RunKit as a submodule; only the small,
stable set of functions below is needed. Keep in sync with the originals:
  - ps_call, PsCallError, timed_call_wrapper,
    repeat_until_success, adler32sum                    <- FLAF/RunKit/run_tools.py
  - update_kerberos_ticket (FLAF name: update_kinit)    <- FLAF/RunKit/kinit.py
  - get_voms_proxy_info                                <- FLAF/RunKit/grid_tools.py
  - CreateVomsProxy                              <- FLAF/RunKit/grid_helper_tasks.py
Remote file I/O reuses FLAF's gfal-CLI file interface (dsprod/grid_tools.py +
dsprod/law_gfal.py + dsprod/law_wlcg.py), so it works on grid (CRAB) workers where
the gfal2 python module is unavailable but the gfal-* CLIs are.
"""

import datetime
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import zlib
from threading import Timer

import law
import luigi


class PsCallError(RuntimeError):
    def __init__(self, cmd_str, return_code, additional_message=None):
        msg = f'Error while running "{cmd_str}".'
        if return_code is not None:
            msg += f" Error code: {return_code}"
        if additional_message is not None:
            msg += f" {additional_message}"
        super(PsCallError, self).__init__(msg)
        self.cmd_str = cmd_str
        self.return_code = return_code
        self.message = additional_message


def ps_call(
    cmd,
    shell=False,
    catch_stdout=False,
    catch_stderr=False,
    decode=True,
    split=None,
    print_output=False,
    expected_return_codes=[0],
    env=None,
    cwd=None,
    timeout=None,
    singularity_cmd=None,
    verbose=0,
):
    if isinstance(cmd, str):
        cmd = [cmd]
    if shell:
        if not (isinstance(cmd, list) and len(cmd) == 1):
            raise ValueError(
                "cmd must be a string or a list with a single element when shell=True"
            )
    if singularity_cmd is not None:
        env_list = []
        if env is not None:
            for key in ["PATH", "LD_LIBRARY_PATH"]:
                if key in env:
                    env_list.append(f'{key}="{env[key]}"')
        if shell:
            env_str = " ".join(env_list)
            if len(env_str) > 0:
                env_str += " "
            full_cmd = [f"{singularity_cmd} --command-to-run '{env_str}{cmd[0]}'"]
        else:
            full_cmd = [singularity_cmd, "--command-to-run", "env"] + env_list + cmd
    else:
        full_cmd = cmd
    cmd_str = []
    for s in cmd:
        if " " in s and not shell:
            s = f"'{s}'"
        cmd_str.append(s)
    cmd_str = " ".join(cmd_str)
    if verbose > 0:
        if singularity_cmd is not None:
            print(f"Entering {singularity_cmd} ...", file=sys.stderr)
        print(f">> {cmd_str}", file=sys.stderr)
    kwargs = {
        "shell": shell,
    }
    if catch_stdout:
        kwargs["stdout"] = subprocess.PIPE
    if catch_stderr:
        if print_output:
            kwargs["stderr"] = subprocess.STDOUT
        else:
            kwargs["stderr"] = subprocess.PIPE
    if env is not None:
        kwargs["env"] = env
    if cwd is not None:
        kwargs["cwd"] = cwd

    # psutil.Process.children does not work.
    def kill_proc(pid):
        child_list = subprocess.run(
            ["ps", "h", "--ppid", str(pid)], capture_output=True, encoding="utf-8"
        )
        for line in child_list.stdout.split("\n"):
            child_info = line.split(" ")
            child_info = [s for s in child_info if len(s) > 0]
            if len(child_info) > 0:
                child_pid = child_info[0]
                kill_proc(child_pid)
        subprocess.run(["kill", "-9", str(pid)], capture_output=True)

    proc = subprocess.Popen(full_cmd, **kwargs)

    def kill_main_proc():
        print(f"\nTimeout is reached while running:\n\t{cmd_str}", file=sys.stderr)
        print("Killing process tree...", file=sys.stderr)
        print(f"Main process PID = {proc.pid}", file=sys.stderr)
        kill_proc(proc.pid)

    timer = Timer(timeout, kill_main_proc) if timeout is not None else None
    try:
        if timer is not None:
            timer.start()
        if catch_stdout and print_output:
            output = b""
            err = b""
            for line in proc.stdout:
                output += line
                print(line.decode("utf-8"), end="")
            proc.stdout.close()
            proc.wait()
        else:
            output, err = proc.communicate()
    finally:
        if timer is not None:
            timer.cancel()
    if (
        expected_return_codes is not None
        and proc.returncode not in expected_return_codes
    ):
        raise PsCallError(cmd_str, proc.returncode)
    if decode:
        if catch_stdout:
            output_decoded = output.decode("utf-8")
            output = output_decoded if split is None else output_decoded.split(split)
        if catch_stderr:
            err_decoded = err.decode("utf-8")
            err = err_decoded if split is None else err_decoded.split(split)

    return proc.returncode, output, err


def repeat_until_success(
    fn, opt_list=([],), exception=None, n_retries=4, retry_sleep_interval=10, verbose=1
):
    for n in range(n_retries):
        for opt in opt_list:
            try:
                fn(*opt)
                return True
            except Exception:
                if verbose > 0:
                    print(traceback.format_exc())
        if n != n_retries - 1:
            sleep_interval = retry_sleep_interval ** (n + 1)
            if verbose > 0:
                print(f"Waiting for {sleep_interval} seconds before the next try.")
            time.sleep(sleep_interval)
    if exception is not None:
        raise exception
    return False


def adler32sum(file_name):
    block_size = 256 * 1024 * 1024
    asum = 1
    with open(file_name, "rb") as f:
        while data := f.read(block_size):
            asum = zlib.adler32(data, asum)
    return asum


def update_kerberos_ticket(verbose=1):
    """Renew the Kerberos ticket and the AFS token (FLAF/RunKit/kinit.py:update_kinit).

    Never raises: it runs for hours inside polling loops, where losing the loop is worse than
    a failed renewal. `aklog` is what actually refreshes the AFS token law writes through.
    """
    if shutil.which("kinit"):
        ps_call(["kinit", "-R"], expected_return_codes=None, verbose=verbose)
    if shutil.which("aklog"):
        ps_call(["aklog"], expected_return_codes=None, verbose=verbose)


class ResyncExistingBranchesProxy:
    """Remote-workflow-proxy mixin: re-check which branch outputs exist before submitting.

    law records that set when luigi *schedules* the workflow (`process_resources`), which happens
    before the workflow's own requirements have run, and never refreshes it afterwards. A task
    whose requirement produces some of its own outputs -- `MakeGridpack`, whose `ImportGridpack`
    requirement copies in every gridpack the store already has -- would otherwise submit a job per
    branch and redo all of that work.
    """

    def run(self):
        self._existing_branches = None
        self._skip_jobs.clear()
        return super(ResyncExistingBranchesProxy, self).run()


class StopOnMassInitialRetryProxy:
    """Remote-workflow-proxy mixin: refuse to regenerate a production whose outputs are gone.

    When a run picks up an existing submission, law re-checks the outputs of every job it had
    recorded as finished and retries the ones whose outputs are missing ("initially missing task
    outputs", `law/workflow/remote.py`). For a few files deleted by hand that is exactly right.
    For a production whose intermediates are consumed downstream it is a catastrophe: one restart
    resubmitted all 8300 `RunProd` jobs of an era, because `NanoMergeTask` had merged their nano
    files and deleted them, as it is meant to.

    The `produced/` records now keep that from happening (see `Task.produced_nano_target`), but a
    storage outage during the check looks identical from here, so a check that condemns most of
    the workflow stops the run rather than acting on it.
    """

    #: share of a resumed workflow's jobs that may be retried for missing outputs in one go
    max_initial_retry_fraction = 0.1

    #: law's error for a job it recorded as finished whose outputs are no longer there
    missing_outputs_error = "initially missing task outputs"

    def submit(self, retry_jobs=None):
        self.stop_on_mass_initial_retry(retry_jobs)
        return super(StopOnMassInitialRetryProxy, self).submit(retry_jobs)

    def stop_on_mass_initial_retry(self, retry_jobs):
        """Raise instead of resubmitting, when most of a resumed workflow lost its outputs."""
        if not retry_jobs or not self._submitted:
            return
        n_missing = sum(
            1
            for job_num in retry_jobs
            if (self.job_data.jobs.get(job_num) or {}).get("error")
            == self.missing_outputs_error
        )
        n_jobs = len(self.job_data)
        if n_missing < 2 or n_missing <= self.max_initial_retry_fraction * n_jobs:
            return
        raise Exception(
            f"{n_missing} of {n_jobs} jobs recorded as finished no longer have their outputs, "
            "so this run would regenerate most of the sample. Nothing was submitted.\n"
            "  - if the outputs were consumed downstream (NanoMergeTask deletes each nano file "
            "it merges), those seeds are done and their `produced/` records are what says so -- "
            "check that the records exist before doing anything else;\n"
            "  - if the storage was unreachable while the outputs were checked, run again once "
            "it is back;\n"
            "  - to redo the work deliberately, delete this workflow's submission file under "
            "data/jobs/ and start again."
        )


def timed_call_wrapper(fn, update_interval, verbose=0):
    last_update = None

    def update(*args, **kwargs):
        nonlocal last_update
        now = datetime.datetime.now()
        delta_t = (
            (now - last_update).total_seconds()
            if last_update is not None
            else float("inf")
        )
        if verbose > 0:
            print(f"timed_call for {fn.__name__}: delta_t = {delta_t} seconds")
        if delta_t >= update_interval:
            fn(*args, **kwargs)
            last_update = now

    return update


def get_voms_proxy_info():
    _, output, _ = ps_call(["voms-proxy-info"], catch_stdout=True, split="\n")
    info = {}
    for line in output:
        if len(line) == 0:
            continue
        match = re.match(r"^(.+) : (.+)", line)
        key = match.group(1).strip()
        info[key] = match.group(2)
    if "timeleft" in info:
        h, m, s = info["timeleft"].split(":")
        info["timeleft"] = float(h) + (float(m) + float(s) / 60.0) / 60.0
    return info


def on_batch_node():
    """True inside a law remote job (HTCondor or CRAB); law exports LAW_JOB_HOME there."""
    return bool(os.getenv("LAW_JOB_HOME"))


def submitted_task_family():
    """Family of the task this `law run` was launched for, or None if it cannot be determined.

    Lets a task tell "I am what was submitted" from "I am a requirement of what was submitted" --
    on a worker the two are otherwise indistinguishable. `law run <module>.<Class>` is resolved to
    the plain family before luigi sees it (law/cli/run.py), so this returns e.g. "MakeGridpack".
    """
    parser = luigi.cmdline_parser.CmdlineParser.get_instance()
    root = getattr(getattr(parser, "known_args", None), "root_task", None)
    return str(root).rsplit(".", 1)[-1] if root else None


class CreateVomsProxy(law.Task):
    time_limit = luigi.Parameter(default="24")

    def __init__(self, *args, **kwargs):
        super(CreateVomsProxy, self).__init__(*args, **kwargs)
        self.proxy_path = os.getenv("X509_USER_PROXY")

    @property
    def on_batch_node(self):
        return on_batch_node()

    def complete(self):
        if not os.path.exists(self.proxy_path):
            return False
        try:
            timeleft = get_voms_proxy_info().get("timeleft", 0.0)
        except PsCallError:
            # voms-proxy-info exits non-zero on an expired or unreadable proxy
            return False
        if self.on_batch_node:
            # Any valid proxy the batch system delegated will do: its remaining lifetime is not
            # ours to police (CRAB's lives for slightly under 24 h, i.e. below the interactive
            # renewal threshold), and voms-proxy-init cannot run unattended on a worker. Enforcing
            # the threshold here used to delete the CRAB proxy, after which every remote-storage
            # call in the job failed with `Error while running "voms-proxy-info"`.
            return True
        return timeleft >= float(self.time_limit)

    def output(self):
        return law.LocalFileTarget(self.proxy_path)

    def create_proxy(self, proxy_file):
        self.publish_message("Creating voms proxy...")
        proxy_file.makedirs()
        ps_call(
            [
                "voms-proxy-init",
                "-voms",
                "cms",
                "-rfc",
                "-valid",
                "192:00",
                "--out",
                proxy_file.path,
            ]
        )

    def run(self):
        if self.on_batch_node:
            raise RuntimeError(
                f"No usable voms proxy at {self.proxy_path} on a batch node, and a new one "
                "cannot be created there. Check that the batch system delegated a proxy."
            )
        proxy_file = self.output()
        if proxy_file.exists():
            self.publish_message("Removing old proxy.")
            proxy_file.remove()
        self.create_proxy(proxy_file)
        if not proxy_file.exists():
            raise RuntimeError("Unable to create voms proxy")
