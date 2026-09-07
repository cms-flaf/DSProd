#!/usr/bin/env python3
"""What the `crab` wrapper does to $HOME, and what that costs the one command law does not drive.

The wrapper moves $HOME off AFS because CRAB rewrites ~/.crab3 on every command and a lapsed AFS
token then kills a running production. The home it moves to is under /tmp, which on lxplus is
node-local: whatever a user puts there by hand exists on that one node and nowhere else. So
`crab createmyproxy` -- which the CRAB gate tells the user to run, and which law never drives --
resolves ~/.globus to nothing and cannot find the GRID certificate.

The fix has to be a symlink and not `export X509_USER_CERT=...`. Those two variables sit ahead of
the default proxy in the GSI credential search order, so exporting them re-points every GSI client
in the process at the *encrypted* user key: the `myproxy-info` that `createmyproxy` runs straight
after a successful delegation then fails with "unable to get passphrase ... interrupted or
cancelled", and CRAB reports the delegation as failed when it in fact succeeded. That regression is
what `test_the_gsi_credential_variables_are_left_alone` pins down.
"""

import io
import os
import re
import shutil
import subprocess
import tempfile
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_SH = os.path.join(dsprod_repo, "env.sh")

#: the wrapper ends by handing over to the real client; stop there and dump what it set up
HANDOVER = 'exec /cvmfs/cms.cern.ch/common/crab "$@"'
# Kept as separate statements rather than one $(...) chain: escaped quotes inside a command
# substitution reach `[` as literal quote characters and silently test the wrong path.
DUMP = "\n".join(
    [
        'echo "HOME=$HOME"',
        'echo "CERT=${X509_USER_CERT-<unset>}"',
        'echo "KEY=${X509_USER_KEY-<unset>}"',
        'if [ -L "$HOME/.globus" ]; then echo ISLINK=yes; else echo ISLINK=no; fi',
        'if [ -e "$HOME/.globus/usercert.pem" ]; then',
        '  echo "RESOLVED=$(readlink -f "$HOME/.globus/usercert.pem")"',
        "else echo 'RESOLVED=<none>'; fi",
    ]
)


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
        # dropping the certificate handling merge green.
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
        for var in ("X509_USER_CERT", "X509_USER_KEY"):
            env.pop(var, None)
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
            if re.match(r"^(HOME|CERT|KEY|ISLINK|RESOLVED)=", line)
        )

    def test_the_certificate_stays_reachable_from_the_real_home(self):
        """The first regression: the scratch home is node-local and has no .globus of its own."""
        globus = self.with_globus()
        seen = self.run_wrapper()
        self.assertEqual(seen["ISLINK"], "yes")
        self.assertEqual(seen["RESOLVED"], os.path.join(globus, "usercert.pem"))
        self.assertTrue(
            os.path.isfile(seen["RESOLVED"]), "the path must actually resolve"
        )

    def test_the_gsi_credential_variables_are_left_alone(self):
        """The second regression: these outrank the default proxy for every GSI client."""
        self.with_globus()
        seen = self.run_wrapper()
        self.assertEqual(seen["CERT"], "<unset>")
        self.assertEqual(seen["KEY"], "<unset>")

    def test_home_is_still_moved_off_the_real_home(self):
        """The invariant the wrapper exists for -- ~/.crab3 must not land on AFS."""
        self.with_globus()
        self.assertEqual(self.run_wrapper()["HOME"], self.scratch)

    def test_a_stale_link_is_repointed(self):
        """A scratch home carried over from another checkout must not keep a wrong target."""
        globus = self.with_globus()
        os.makedirs(self.scratch)
        os.symlink(
            os.path.join(self.tmp, "gone"), os.path.join(self.scratch, ".globus")
        )
        self.assertEqual(
            self.run_wrapper()["RESOLVED"], os.path.join(globus, "usercert.pem")
        )

    def test_a_real_directory_is_never_clobbered(self):
        """If someone put a real .globus in the scratch home, leave it exactly as it is."""
        self.with_globus()
        os.makedirs(os.path.join(self.scratch, ".globus"))
        write(os.path.join(self.scratch, ".globus", "usercert.pem"), "theirs")
        seen = self.run_wrapper()
        self.assertEqual(seen["ISLINK"], "no")
        self.assertEqual(
            seen["RESOLVED"], os.path.join(self.scratch, ".globus", "usercert.pem")
        )

    def test_no_globus_means_no_dangling_link(self):
        seen = self.run_wrapper()
        self.assertEqual(seen["ISLINK"], "no")
        self.assertEqual(seen["RESOLVED"], "<none>")


if __name__ == "__main__":
    unittest.main()
