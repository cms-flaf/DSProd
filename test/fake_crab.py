"""A scripted `crab` executable for driving law's real CrabJobManager.

law runs `crab` in a subprocess, with the CMSSW sandbox environment as its whole environment, and
parses what it prints. Replacing that executable -- and the sandbox environment, which needs
cvmfs -- is the narrowest double there is: law builds the command, checks the project directory,
runs the process, handles its exit code and parses its output exactly as in production.

Imported by bare name: `unittest discover -s test` puts this directory on `sys.path`, where a
`test.` prefix would resolve to CPython's own `test` package.
"""

import os
import shutil
import tempfile
from unittest import mock

import law

law.contrib.load("cms")

_SCRIPT = r"""#!/bin/bash
echo "$*" >> "$FAKE_CRAB_HOME/calls"
case "$1" in
  submit)
    proj="$(cat "$FAKE_CRAB_HOME/next_submission")"
    mkdir -p "$proj"
    name="$(basename "$proj")"
    printf 'config.Data.totalUnits = %s\n' "$(cat "$FAKE_CRAB_HOME/next_n_jobs")" > "$proj/crab.log"
    printf 'DEBUG 2026-10-08 Task name: 261008_120000:someone_%s\n' "$name" >> "$proj/crab.log"
    echo "Task name: 261008_120000:someone_$name"
    echo "Log file is $proj/crab.log"
    ;;
  status)
    while [ "$#" -gt 0 ] && [ "$1" != "--dir" ]; do shift; done
    answer="$FAKE_CRAB_HOME/answers/$(basename "$2")"
    cat "$answer.out"
    exit "$(cat "$answer.rc" 2>/dev/null || echo 0)"
    ;;
esac
"""


class FakeCrab:
    """`crab` on the PATH law runs it with, answering `status` per project as scripted."""

    def __init__(self, case):
        self.home = tempfile.mkdtemp(prefix="dsprod_fake_crab_")
        case.addCleanup(shutil.rmtree, self.home, True)
        os.makedirs(os.path.join(self.home, "answers"))
        bin_dir = os.path.join(self.home, "bin")
        os.makedirs(bin_dir)
        script = os.path.join(bin_dir, "crab")
        with open(script, "w") as f:
            f.write(_SCRIPT)
        os.chmod(script, 0o755)
        self.env = {
            "PATH": f"{bin_dir}:{os.environ.get('PATH', '/usr/bin:/bin')}",
            "FAKE_CRAB_HOME": self.home,
        }
        patcher = mock.patch.object(
            law.cms.CrabJobManager,
            "cmssw_env",
            new_callable=mock.PropertyMock,
            return_value=self.env,
        )
        patcher.start()
        case.addCleanup(patcher.stop)

    def project(self, name):
        """A real project directory -- law refuses to query one that does not exist."""
        path = os.path.join(self.home, "projects", name)
        os.makedirs(path)
        return path

    def answer(self, proj_dir, out, code=0):
        """What `crab status --dir <proj_dir>` prints, and its exit code."""
        base = os.path.join(self.home, "answers", os.path.basename(proj_dir))
        with open(base + ".out", "w") as f:
            f.write(out)
        with open(base + ".rc", "w") as f:
            f.write(str(code))

    def next_submission(self, proj_dir, n_jobs):
        """Where the next `crab submit` creates its project, and how many jobs it holds."""
        with open(os.path.join(self.home, "next_submission"), "w") as f:
            f.write(proj_dir)
        with open(os.path.join(self.home, "next_n_jobs"), "w") as f:
            f.write(str(n_jobs))

    @property
    def calls(self):
        try:
            with open(os.path.join(self.home, "calls")) as f:
                return f.read().splitlines()
        except FileNotFoundError:
            return []
