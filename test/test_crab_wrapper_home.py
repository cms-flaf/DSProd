#!/usr/bin/env python3
"""What the `crab` wrapper does to $HOME, and what that costs the one command law does not drive.

The wrapper moves $HOME off AFS because CRAB rewrites ~/.crab3 on every command and a lapsed AFS
token then kills a running production. The home it moves to is under /tmp, which on lxplus is
node-local: whatever a user puts there by hand exists on that one node and nowhere else. So the
redirection may not be the only thing the wrapper does to the certificate path -- `crab
createmyproxy`, which the CRAB gate tells the user to run and which law never calls with --proxy,
reads the GRID certificate from ~/.globus and finds an empty directory there.
"""

import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_SH = os.path.join(dsprod_repo, "env.sh")

#: the wrapper ends by handing over to the real client; stop there and dump the environment
HANDOVER = 'exec /cvmfs/cms.cern.ch/common/crab "$@"'
DUMP = 'echo "HOME=$HOME"; echo "CERT=${X509_USER_CERT-<unset>}"; echo "KEY=${X509_USER_KEY-<unset>}"'


def write(path, text):
    with io.open(path, "w", encoding="utf-8") as f:
        f.write(text)


def crab_wrapper_body():
    """The `crab` wrapper exactly as env.sh writes it, with the hand-over replaced by a dump."""
    with io.open(ENV_SH, encoding="utf-8") as f:
        src = f.read()
    body = re.search(r"<<'CRABWRAP'\n(.*?)\nCRABWRAP\n", src, re.S).group(1)
    assert HANDOVER in body, "the wrapper no longer ends by exec'ing the real client"
    return body.replace(HANDOVER, DUMP)


class CrabWrapperHome(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # No cvmfs guard: the wrapper's `source /cvmfs/.../cmsset_default.sh` is its only cvmfs
        # dependency and a failed `source` does not abort bash, so the body runs to the end on a
        # bare CI runner too. Guarding on cvmfs would skip the whole class in the only place that
        # runs this suite automatically (unit-tests.yaml, ubuntu-latest) and let a later edit
        # dropping the certificate block merge green.
        cls.body = crab_wrapper_body()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.real_home = os.path.join(self.tmp, "realhome")
        self.scratch = os.path.join(self.tmp, "scratch")
        os.makedirs(self.real_home)
        self.wrapper = os.path.join(self.tmp, "crab")
        write(self.wrapper, self.body)
        os.chmod(self.wrapper, 0o755)

    def with_globus(self):
        globus = os.path.join(self.real_home, ".globus")
        os.makedirs(globus)
        for name in ("usercert.pem", "userkey.pem"):
            write(os.path.join(globus, name), "x")
        return globus

    def run_wrapper(self, **extra):
        env = dict(os.environ)
        env.pop("X509_USER_CERT", None)
        env.pop("X509_USER_KEY", None)
        env["HOME"] = self.real_home
        env["DSPROD_CRAB_HOME"] = self.scratch
        env["ANALYSIS_PATH"] = self.tmp
        env.update(extra)
        out = subprocess.run(
            [self.wrapper, "createmyproxy"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            universal_newlines=True,
            timeout=300,
        ).stdout
        return dict(
            line.split("=", 1)
            for line in out.strip().split("\n")
            if re.match(r"^(HOME|CERT|KEY)=", line)
        )

    def test_the_certificate_stays_reachable_from_the_real_home(self):
        """The regression: the scratch home is node-local and has no .globus of its own."""
        globus = self.with_globus()
        seen = self.run_wrapper()
        self.assertEqual(seen["CERT"], os.path.join(globus, "usercert.pem"))
        self.assertEqual(seen["KEY"], os.path.join(globus, "userkey.pem"))
        self.assertTrue(os.path.isfile(seen["CERT"]), "the path must actually resolve")

    def test_home_is_still_moved_off_the_real_home(self):
        """The invariant the wrapper exists for -- ~/.crab3 must not land on AFS."""
        self.with_globus()
        self.assertEqual(self.run_wrapper()["HOME"], self.scratch)

    def test_an_explicit_certificate_is_not_overridden(self):
        self.with_globus()
        mine = os.path.join(self.tmp, "mycert.pem")
        write(mine, "x")
        self.assertEqual(self.run_wrapper(X509_USER_CERT=mine)["CERT"], mine)

    def test_no_globus_means_no_invented_path(self):
        """Nothing to point at -- leave it unset rather than export a path that does not exist."""
        self.assertEqual(self.run_wrapper()["CERT"], "<unset>")


if __name__ == "__main__":
    unittest.main()
