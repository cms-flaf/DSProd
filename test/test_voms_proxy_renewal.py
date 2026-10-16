#!/usr/bin/env python3
"""Renewing the VOMS proxy on the submit node never leaves the run without one.

`CreateVomsProxy` renews a proxy with less than `time_limit` hours left by running
`voms-proxy-init`, which asks for the certificate passphrase. It used to remove the old proxy
first, so a driver without a controlling terminal (setsid, CI, an agent) lost a proxy that was
still valid for hours when the passphrase prompt failed (the DSProd #55 readiness run,
2026-10-15). The old proxy is now replaced only once the new one exists. `run()` is driven for
real, through `ps_call`, against a stand-in `voms-proxy-init` placed first on PATH.
"""

import itertools
import os
import sys
import tempfile
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod.tools import CreateVomsProxy  # noqa: E402

#: stands in for voms-proxy-init: writes NEW to the --out path; with FAKE_VOMS_FAIL set it leaves
#: a partial file and fails like a passphrase prompt without a terminal; with FAKE_VOMS_SILENT it
#: exits 0 without writing anything
FAKE_VOMS_PROXY_INIT = """#!/bin/bash
out=""
while [ $# -gt 0 ]; do
    if [ "$1" = "--out" ]; then out="$2"; shift; fi
    shift
done
if [ -n "$FAKE_VOMS_FAIL" ]; then
    echo PARTIAL > "$out"
    echo "Enter GRID pass phrase for this identity:" >&2
    echo "Error: could not read the pass phrase" >&2
    exit 1
fi
if [ -n "$FAKE_VOMS_SILENT" ]; then
    exit 0
fi
echo NEW > "$out"
"""

_time_limits = itertools.count(1000)


class RenewingOnTheSubmitNode(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="dsprod_voms_renewal_test_")
        bin_dir = os.path.join(self.dir, "bin")
        os.makedirs(bin_dir)
        fake = os.path.join(bin_dir, "voms-proxy-init")
        with open(fake, "w") as f:
            f.write(FAKE_VOMS_PROXY_INIT)
        os.chmod(fake, 0o755)
        self.proxy = os.path.join(self.dir, "voms.proxy")
        with open(self.proxy, "w") as f:
            f.write("OLD\n")
        env = {
            "PATH": bin_dir + os.pathsep + os.environ.get("PATH", ""),
            "X509_USER_PROXY": self.proxy,
        }
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)
        for var in ("LAW_JOB_HOME", "FAKE_VOMS_FAIL", "FAKE_VOMS_SILENT"):
            os.environ.pop(var, None)

    def tearDown(self):
        for root, dirs, files in os.walk(self.dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.dir)

    def task(self):
        # a fresh parameter value per test: luigi caches instances by parameters, and the
        # proxy path is read when the instance is created
        return CreateVomsProxy(time_limit=str(next(_time_limits)))

    def leftovers(self):
        return sorted(n for n in os.listdir(self.dir) if n.startswith("voms.proxy.new"))

    def read_proxy(self):
        with open(self.proxy) as f:
            return f.read()

    def test_a_failed_renewal_keeps_the_old_proxy(self):
        os.environ["FAKE_VOMS_FAIL"] = "1"
        with self.assertRaisesRegex(RuntimeError, "existing one .* is kept"):
            self.task().run()
        self.assertEqual(self.read_proxy(), "OLD\n")
        self.assertEqual(self.leftovers(), [])

    def test_a_failed_first_creation_says_there_is_no_proxy(self):
        os.remove(self.proxy)
        os.environ["FAKE_VOMS_FAIL"] = "1"
        with self.assertRaisesRegex(RuntimeError, "there is none at"):
            self.task().run()
        self.assertFalse(os.path.exists(self.proxy))
        self.assertEqual(self.leftovers(), [])

    def test_a_renewal_that_writes_nothing_keeps_the_old_proxy(self):
        os.environ["FAKE_VOMS_SILENT"] = "1"
        with self.assertRaisesRegex(RuntimeError, "Unable to create voms proxy"):
            self.task().run()
        self.assertEqual(self.read_proxy(), "OLD\n")
        self.assertEqual(self.leftovers(), [])

    def test_a_successful_renewal_replaces_the_old_proxy(self):
        self.task().run()
        self.assertEqual(self.read_proxy(), "NEW\n")
        self.assertEqual(self.leftovers(), [])

    def test_a_first_proxy_is_created(self):
        os.remove(self.proxy)
        self.task().run()
        self.assertEqual(self.read_proxy(), "NEW\n")
        self.assertEqual(self.leftovers(), [])

    def test_a_batch_node_never_renews(self):
        os.environ["LAW_JOB_HOME"] = self.dir
        with self.assertRaisesRegex(RuntimeError, "batch node"):
            self.task().run()
        self.assertEqual(self.read_proxy(), "OLD\n")


if __name__ == "__main__":
    unittest.main()
