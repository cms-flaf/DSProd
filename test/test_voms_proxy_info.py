#!/usr/bin/env python3
"""A stale CRL must not abort a job that still has a usable proxy.

On 2026-09-29, 309 of 1400 jobs of `crab_RunProd_Run3_XHHbbtautau_9c8d103e` died
during scheduling. `voms-proxy-info` exited 1 with `CRL has expired` for
`cms-auth.cern.ch`, and `get_voms_proxy_info()` raised that as `PsCallError`.
`GFALFileInterface` calls it from `RunProd.output()`, so the job never reached
cmsRun. The read now passes `-dont-verify-ac`: the callers need the proxy path
and its remaining lifetime, not the attribute-certificate check.

A missing proxy still exits non-zero. That failure has to keep raising, with
the command's stderr in the message. Treating every exit 1 as success would
hide `Couldn't find a valid proxy.`
"""

import os
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod.tools import PsCallError, get_voms_proxy_info  # noqa: E402

STDOUT = [
    "subject   : /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=kandrosov",
    "path      : /tmp/x509up_u34016",
    "timeleft  : 23:59:00",
    "",
]


class VomsProxyInfoTests(unittest.TestCase):
    def test_ac_verification_is_skipped_and_the_proxy_is_parsed(self):
        with mock.patch("dsprod.tools.ps_call", return_value=(0, STDOUT, [])) as call:
            info = get_voms_proxy_info()
        call.assert_called_once_with(
            ["voms-proxy-info", "-dont-verify-ac"],
            catch_stdout=True,
            catch_stderr=True,
            split="\n",
        )
        self.assertEqual(info["path"], "/tmp/x509up_u34016")
        self.assertAlmostEqual(info["timeleft"], 23 + 59 / 60.0)

    def test_a_missing_proxy_is_still_an_error(self):
        err = PsCallError(
            "voms-proxy-info -dont-verify-ac",
            1,
            "Couldn't find a valid proxy.",
        )
        with mock.patch("dsprod.tools.ps_call", side_effect=err):
            with self.assertRaises(PsCallError) as caught:
                get_voms_proxy_info()
        self.assertIn("Couldn't find a valid proxy.", str(caught.exception))
        self.assertEqual(caught.exception.return_code, 1)


if __name__ == "__main__":
    unittest.main()
