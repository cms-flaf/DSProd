"""gfal-CLI remote-file helpers, vendored from FLAF/RunKit/grid_tools.py.

Only the gfal cluster is reused here (DSProd does not read datasets via Rucio, so the
Rucio/DAS helpers of the original are omitted). These call the ``gfal-*`` command-line
tools rather than the gfal2 python module, so remote EOS I/O works on CRAB/WLCG workers
where the gfal2 python bindings are not available for the job's python.
"""

import datetime
import os
import re

from .tools import (
    ps_call,
    PsCallError,
    get_voms_proxy_info,
    repeat_until_success,
)

COPY_TMP_SUFFIX = ".tmp"
COPY_TMP_LOCAL_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".gfal_copy_safe_tmp"
)
CHECK_WRITE_SUFFIX = ".check"


class FileInfo:
    def __init__(self, name=None, path=None, size=None, date=None, is_dir=None):
        self.name = name
        self.path = path
        self.size = size
        self.date = date
        self.is_dir = is_dir

    @property
    def full_name(self):
        return os.path.join(self.path, self.name)

    def __str__(self):
        date_str = (
            self.date.strftime("%Y-%m-%dT%H:%M") if self.date is not None else None
        )
        return f'name="{self.name}", path="{self.path}", size={self.size}, date={date_str}, is_dir={self.is_dir}'

    def __repr__(self):
        return self.__str__()


class GfalError(RuntimeError):
    def __init__(self, msg):
        super(GfalError, self).__init__(msg)


def get_voms_proxy_token(voms_token=None):
    if voms_token is None:
        return get_voms_proxy_info()["path"]
    return voms_token


def create_tmp_local_file():
    if not os.path.exists(COPY_TMP_LOCAL_FILE):
        with open(COPY_TMP_LOCAL_FILE, "w") as f:
            f.write("0")
    return COPY_TMP_LOCAL_FILE


def gfal_env(voms_token):
    return {"X509_USER_PROXY": voms_token, "GFAL_PYTHONBIN": "/usr/bin/python3"}


def gfal_copy(
    input_file,
    output_file,
    voms_token=None,
    number_of_streams=2,
    timeout=7200,
    verbose=1,
):
    voms_token = get_voms_proxy_token(voms_token)
    try:
        catch_output = verbose == 0
        cmd = [
            "gfal-copy",
            "--parent",
            "--recursive",
            "--nbstreams",
            str(number_of_streams),
            "--timeout",
            str(timeout),
        ]
        if verbose > 1:
            n_v = min(3, verbose - 1)
            cmd.append("-" + "v" * n_v)
        cmd.extend([input_file, output_file])
        ps_call(
            cmd,
            shell=False,
            env=gfal_env(voms_token),
            verbose=verbose,
            catch_stdout=catch_output,
            catch_stderr=catch_output,
        )
    except PsCallError as e:
        raise GfalError(
            f'gfal_copy: unable to copy "{input_file}" to "{output_file}"\n{e}'
        ) from None


def gfal_copy_safe(
    input_file,
    output_file,
    voms_token=None,
    number_of_streams=2,
    timeout=7200,
    expected_adler32sum=None,
    n_retries=4,
    retry_sleep_interval=10,
    copy_mode="copy_flag",
    verbose=1,
):
    voms_token = get_voms_proxy_token(voms_token)
    if expected_adler32sum is None:
        try:
            stat = gfal_stat(input_file, voms_token=voms_token)
            if stat["type"] == "regular file":
                expected_adler32sum = gfal_sum(
                    input_file, voms_token=voms_token, sum_type="adler32"
                )
        except GfalError as e:
            if verbose > 0:
                print(f'WARNING: gfal_sum failed for "{input_file}".\n{e}')
    if copy_mode not in ["copy_rename", "copy_flag"]:
        raise RuntimeError(f'gfal_copy_safe: unknown copy mode "{copy_mode}".')
    if copy_mode == "copy_flag":
        tmp_local_file = create_tmp_local_file()
    output_file_tmp = output_file + COPY_TMP_SUFFIX
    output_file_sum_target = (
        output_file if copy_mode == "copy_flag" else output_file_tmp
    )
    attempt = -1

    def download():
        nonlocal attempt
        attempt += 1
        active_verbose = min(verbose + attempt if verbose > 0 else 0, 2)
        if gfal_exists(output_file, voms_token=voms_token):
            gfal_rm(output_file, voms_token=voms_token, recursive=False)
        if gfal_exists(output_file_tmp, voms_token=voms_token):
            gfal_rm(output_file_tmp, voms_token=voms_token, recursive=False)
        if copy_mode == "copy_flag":
            gfal_copy(
                tmp_local_file,
                output_file_tmp,
                voms_token=voms_token,
                number_of_streams=number_of_streams,
                timeout=timeout,
                verbose=active_verbose,
            )
            gfal_copy(
                input_file,
                output_file,
                voms_token=voms_token,
                number_of_streams=number_of_streams,
                timeout=timeout,
                verbose=active_verbose,
            )
        elif copy_mode == "copy_rename":
            gfal_copy(
                input_file,
                output_file_tmp,
                voms_token=voms_token,
                number_of_streams=number_of_streams,
                timeout=timeout,
                verbose=active_verbose,
            )
        if expected_adler32sum is not None:
            output_adler32sum = gfal_sum(
                output_file_sum_target, voms_token=voms_token, sum_type="adler32"
            )
            if output_adler32sum != expected_adler32sum:
                raise GfalError(
                    f'Failed adler32sum check for "{output_file_sum_target}".'
                    f" {output_adler32sum:x} != {expected_adler32sum:x}."
                )
        if copy_mode == "copy_flag":
            gfal_rm(output_file_tmp, voms_token=voms_token, recursive=False)
        elif copy_mode == "copy_rename":
            gfal_rename(output_file_tmp, output_file, voms_token=voms_token)
            if not gfal_exists(output_file, voms_token=voms_token):
                raise GfalError(
                    f'Failed to rename "{output_file_tmp}" to "{output_file}".'
                )

    repeat_until_success(
        download,
        n_retries=n_retries,
        retry_sleep_interval=retry_sleep_interval,
        verbose=verbose,
        exception=GfalError(f'Unable to copy "{input_file}" to "{output_file}".'),
    )


def gfal_ls(path, voms_token=None, catch_stderr=False, verbose=1):
    voms_token = get_voms_proxy_token(voms_token)
    try:
        _, output, _ = ps_call(
            ["gfal-ls", "--long", "--all", "--time-style", "long-iso", path],
            shell=False,
            env=gfal_env(voms_token),
            catch_stdout=True,
            catch_stderr=catch_stderr,
            split="\n",
            verbose=verbose,
        )
    except PsCallError as e:
        raise GfalError(f'gfal_ls: unable to list "{path}"\n{e}') from None
    files = []
    for line in output:
        if len(line) == 0:
            continue
        items = re.match(
            r"^([rwx\-d]+) +[0-9]+ +[0-9]+ +[0-9]+ +([0-9]+) +([0-9\-]+ [0-9:]+) +(.*)$",
            line,
        )
        if items is None:
            raise GfalError(f'gfal_ls: unable to parse "{line}"')
        file = FileInfo()
        file.name = items.group(4).strip()
        if file.name in [".", ".."]:
            continue
        if file.name == path:
            file.path, file.name = os.path.split(path)
        else:
            file.path = path
        file.size = int(items.group(2))
        file.date = datetime.datetime.strptime(items.group(3), "%Y-%m-%d %H:%M")
        file.is_dir = items.group(1).startswith("d")
        files.append(file)
    return files


def gfal_ls_recursive(path, voms_token=None, verbose=1):
    voms_token = get_voms_proxy_token(voms_token)
    all_files = []
    path_files = gfal_ls(path, voms_token=voms_token, verbose=verbose)
    for file in path_files:
        all_files.append(file)
        if file.is_dir:
            all_files.extend(
                gfal_ls_recursive(
                    file.full_name, voms_token=voms_token, verbose=verbose
                )
            )
    return sorted(set(all_files), key=lambda f: f.full_name)


def gfal_ls_safe(path, voms_token=None, catch_stderr=False, verbose=1):
    try:
        return gfal_ls(
            path, voms_token=voms_token, catch_stderr=catch_stderr, verbose=verbose
        )
    except GfalError:
        return None


def gfal_stat(path, voms_token=None):
    voms_token = get_voms_proxy_token(voms_token)
    result = {"size": None, "type": None}
    try:
        _, stdout, _ = ps_call(
            ["gfal-stat", path],
            shell=False,
            env=gfal_env(voms_token),
            catch_stdout=True,
            catch_stderr=True,
            decode=True,
            split="\n",
        )

        if len(stdout) > 1:
            match = re.match(r"  Size: ([0-9]+) *(.+)", stdout[1])
            if match is not None:
                result["size"] = int(match.group(1))
                result["type"] = match.group(2).strip()
    except PsCallError:
        pass
    return result


def gfal_exists(path, voms_token=None):
    voms_token = get_voms_proxy_token(voms_token)
    try:
        ps_call(
            ["gfal-stat", path],
            shell=False,
            env=gfal_env(voms_token),
            catch_stdout=True,
            catch_stderr=True,
        )
    except PsCallError:
        return False
    return True


def gfal_check_write(path, return_exception=False, voms_token=None, verbose=0):
    voms_token = get_voms_proxy_token(voms_token)
    target_path = path + CHECK_WRITE_SUFFIX
    tmp_local_file = create_tmp_local_file()
    result = (True, None)
    try:
        if gfal_exists(target_path, voms_token=voms_token):
            gfal_rm(target_path, voms_token=voms_token, recursive=False)
        gfal_copy(tmp_local_file, target_path, voms_token=voms_token, verbose=verbose)
        gfal_rm(target_path, voms_token=voms_token, verbose=verbose)
    except GfalError as e:
        result = (False, e)
    if return_exception:
        return result
    return result[0]


def gfal_sum(path, voms_token=None, sum_type="adler32"):
    voms_token = get_voms_proxy_token(voms_token)
    try:
        _, output, _ = ps_call(
            ["gfal-sum", path, sum_type],
            shell=False,
            env=gfal_env(voms_token),
            catch_stdout=True,
        )
        sum_str = output.split(" ")[-1]
        sum_int = int(sum_str, 16)
    except PsCallError as e:
        raise GfalError(
            f'gfal_sum: unable to get {sum_type} for "{path}"\n{e}'
        ) from None
    except ValueError as e:
        raise GfalError(
            f'gfal_sum: unable to parse {sum_type} for "{path}".'
            f"\ngfal-sum output:\n--------\n{output}--------\n{e}"
        ) from None
    return sum_int


def gfal_rm(path, voms_token=None, recursive=False, verbose=0, timeout=1800):
    voms_token = get_voms_proxy_token(voms_token)
    cmd = ["gfal-rm", "-t", str(timeout)]
    if recursive:
        cmd.append("-r")
    cmd.append(path)
    try:
        ps_call(
            cmd,
            shell=False,
            env=gfal_env(voms_token),
            catch_stdout=(verbose == 0),
            verbose=verbose,
        )
    except PsCallError as e:
        raise GfalError(f'gfal_rm: unable to remove "{path}"\n{e}') from None


def gfal_rm_recursive(path, voms_token=None, timeout=86400):
    gfal_rm(path, voms_token=voms_token, recursive=True, verbose=1, timeout=timeout)


def gfal_rename(path, new_path, voms_token=None):
    voms_token = get_voms_proxy_token(voms_token)
    try:
        ps_call(
            ["gfal-rename", path, new_path],
            shell=False,
            env=gfal_env(voms_token),
            catch_stdout=True,
        )
    except PsCallError as e:
        raise GfalError(
            f'gfal_rename: unable to rename "{path}" to "{new_path}"\n{e}'
        ) from None


def path_to_pfn(path, *sub_paths):
    """Resolve a storage base to a PFN. DSProd storage bases are already full protocol
    URLs (davs://, root://, ...), so this is a passthrough with optional sub-path join;
    the site-prefixed ``T<n>:`` Rucio form used by FLAF is not supported here.
    """
    if re.match(r"^T[0-9]", path):
        raise NotImplementedError(
            f"site-prefixed storage bases are not supported in DSProd: {path}"
        )
    return os.path.join(path, *sub_paths)
