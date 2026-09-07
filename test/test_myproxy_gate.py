#!/usr/bin/env python3
"""Which MyProxy credential is allowed to open the CRAB gate.

`crab submit --proxy <file>`, which law always uses, makes CRABClient return from
`handleMyProxy` before it delegates or renews anything, so the credential the
TaskWorker will retrieve is whatever is already on myproxy.cern.ch -- and it is looked up under
`sha1(DN)` and under no other name. A credential stored under the plain DN, which is what a bare
`myproxy-init -d` writes, is therefore not a credential CRAB can use, and letting it open the gate
sends a whole production out to fail on the TaskWorker instead of failing here in a second.
"""

import os
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod.crab import DSProdCrabWorkflowProxy  # noqa: E402

DAY = 24 * 3600
HASHED = "8fdd8487bd5a4293015d1b24a6322b899a54b571"
PLAIN_DN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=someone"


def gate(encoded_timeleft, plain_timeleft, proxy_file):
    """Run `setup_job_manager` against a myproxy server holding these two credentials."""

    def fake_info(encode_username=True, silent=False):
        timeleft = encoded_timeleft if encode_username else plain_timeleft
        if timeleft is None:
            return None
        return {
            "username": HASHED if encode_username else PLAIN_DN,
            "timeleft": timeleft,
        }

    with mock.patch.dict(os.environ, {"X509_USER_PROXY": proxy_file}), mock.patch(
        "law.wlcg.get_myproxy_info", side_effect=fake_info
    ), mock.patch("law.wlcg.check_vomsproxy_validity", return_value=True):
        return DSProdCrabWorkflowProxy.setup_job_manager(mock.Mock())


class MyProxyGate(unittest.TestCase):
    def setUp(self):
        # any readable file passes the `os.path.isfile` check on the VOMS proxy
        self.proxy = os.path.abspath(__file__)

    def test_hashed_credential_opens_the_gate(self):
        kwargs = gate(
            encoded_timeleft=30 * DAY, plain_timeleft=None, proxy_file=self.proxy
        )
        self.assertEqual(kwargs["myproxy_username"], HASHED)
        self.assertEqual(kwargs["proxy"], self.proxy)

    def test_fresh_plain_dn_credential_does_not(self):
        """The regression: a 30-day DN-keyed credential next to a 2-day hashed one."""
        with self.assertRaises(RuntimeError) as caught:
            gate(
                encoded_timeleft=2 * DAY, plain_timeleft=30 * DAY, proxy_file=self.proxy
            )
        self.assertIn("crab createmyproxy", str(caught.exception))

    def test_plain_dn_credential_alone_does_not(self):
        with self.assertRaises(RuntimeError):
            gate(encoded_timeleft=None, plain_timeleft=30 * DAY, proxy_file=self.proxy)

    def test_five_days_is_the_boundary(self):
        self.assertEqual(
            gate(encoded_timeleft=5 * DAY, plain_timeleft=None, proxy_file=self.proxy)[
                "myproxy_username"
            ],
            HASHED,
        )
        with self.assertRaises(RuntimeError):
            gate(
                encoded_timeleft=5 * DAY - 1, plain_timeleft=None, proxy_file=self.proxy
            )

    def test_missing_proxy_file_is_reported_as_the_proxy(self):
        with self.assertRaises(RuntimeError) as caught:
            gate(
                encoded_timeleft=30 * DAY,
                plain_timeleft=None,
                proxy_file="/nonexistent/proxy",
            )
        self.assertIn("X509_USER_PROXY", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
