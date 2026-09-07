#!/usr/bin/env python3
"""Why the `crab` wrapper has to guarantee a CMSSW environment before handing over.

The cvmfs client opens with
`[ -z "$CMSSW_VERSION" ] && echo "CMSSW is missing. You must do cmsenv first" && exit` -- an `exit`
with no code, so declining to run is reported as SUCCESS. The wrapper only cmsenv'd a release from
`$ANALYSIS_PATH/soft`, and a fresh checkout has none: InstallCMSSW builds them during the first
run. That is exactly when `crab createmyproxy` has to work, because the CRAB gate refuses to submit
without the MyProxy credential it creates. Following the install page top to bottom therefore
printed one terse line, exited 0, and left the reader believing the credential existed.

So the wrapper must never hand over with `CMSSW_VERSION` unset. It bootstraps a read-only release
from cvmfs, and where cvmfs is unavailable it must fail loudly instead.
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
CONDITIONS = os.path.join(dsprod_repo, "config", "conditions_Run3.yaml")
HANDOVER = 'exec /cvmfs/cms.cern.ch/common/crab "$@"'
HAVE_CVMFS = os.path.isdir("/cvmfs/cms.cern.ch")


class CrabWrapperCmssw(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with io.open(ENV_SH, encoding="utf-8") as f:
            src = f.read()
        body = re.search(r"<<'CRABWRAP'\n(.*?)\nCRABWRAP\n", src, re.S).group(1)
        assert (
            HANDOVER in body
        ), "the wrapper no longer ends by exec'ing the real client"
        cls.body = body.replace(
            HANDOVER, 'echo "HANDOVER CMSSW_VERSION=${CMSSW_VERSION-}"'
        )

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        # a checkout as it is before the first law run: config present, soft/ empty
        os.makedirs(os.path.join(self.tmp, "soft"))
        os.makedirs(os.path.join(self.tmp, "config"))
        shutil.copy(CONDITIONS, os.path.join(self.tmp, "config"))
        self.wrapper = os.path.join(self.tmp, "crab")
        with io.open(self.wrapper, "w", encoding="utf-8") as f:
            f.write(self.body)
        os.chmod(self.wrapper, 0o755)

    def run_wrapper(self):
        env = dict(os.environ)
        for var in ("CMSSW_VERSION", "CMSSW_BASE", "SCRAM_ARCH"):
            env.pop(var, None)
        env["HOME"] = self.tmp
        env["DSPROD_CRAB_HOME"] = os.path.join(self.tmp, "crabhome")
        env["ANALYSIS_PATH"] = self.tmp
        done = subprocess.run(
            [self.wrapper, "createmyproxy"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=600,
        )
        return done

    def test_it_never_hands_over_without_a_cmssw_environment(self):
        """The regression: handing over with CMSSW_VERSION unset is a silent no-op, exit 0."""
        done = self.run_wrapper()
        handover = re.search(r"^HANDOVER CMSSW_VERSION=(.*)$", done.stdout, re.M)
        if handover:
            self.assertTrue(
                handover.group(1).startswith("CMSSW_"),
                f"handed over with CMSSW_VERSION={handover.group(1)!r} -- the client would "
                "decline to run and report success",
            )
        else:
            self.assertNotEqual(
                done.returncode, 0, "declined to run but reported success"
            )
            self.assertIn("no CMSSW environment", done.stderr)

    @unittest.skipUnless(HAVE_CVMFS, "the cvmfs bootstrap needs /cvmfs/cms.cern.ch")
    def test_a_fresh_checkout_bootstraps_a_release_from_cvmfs(self):
        done = self.run_wrapper()
        handover = re.search(r"^HANDOVER CMSSW_VERSION=(CMSSW_\S+)$", done.stdout, re.M)
        self.assertIsNotNone(handover, f"no hand-over; stderr={done.stderr[:400]}")
        with io.open(CONDITIONS, encoding="utf-8") as f:
            declared = set(
                re.findall(r"CMSSW_[0-9]+_[0-9]+_[0-9]+(?:_patch[0-9]+)?", f.read())
            )
        self.assertIn(
            handover.group(1), declared, "bootstrapped a release DSProd does not use"
        )


if __name__ == "__main__":
    unittest.main()
