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


class CreateVomsProxy(law.Task):
    time_limit = luigi.Parameter(default="24")

    def __init__(self, *args, **kwargs):
        super(CreateVomsProxy, self).__init__(*args, **kwargs)
        self.proxy_path = os.getenv("X509_USER_PROXY")
        if os.path.exists(self.proxy_path):
            proxy_info = get_voms_proxy_info()
            timeleft = proxy_info.get("timeleft", 0.0)
            if timeleft < float(self.time_limit):
                self.publish_message(
                    f"Removing old proxy which expires in a less than {timeleft:.1f} hours."
                )
                self.output().remove()

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
        proxy_file = self.output()
        self.create_proxy(proxy_file)
        if not proxy_file.exists():
            raise RuntimeError("Unable to create voms proxy")
