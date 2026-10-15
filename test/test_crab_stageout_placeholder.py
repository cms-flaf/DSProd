#!/usr/bin/env python3
"""CRAB's Site.storageSite / Data.outLFNDirBase are a placeholder any submitter can use.

CRAB transfers nothing for DSProd, but the two values must pass the CRAB server's validation and
the task worker's Rucio resolution of a write PFN at the site. They used to carry `$USER`
(`/store/user/$USER/DSProd_crab_unused` at T3_CH_CERNBOX), which the server accepts only while
`$USER` is the submitter's CERN username. The LFN rule is checked with CRAB's own `checkOutLFN`,
read from the client the `crab` command runs, and the site with its CMS storage description; where
/cvmfs is not mounted those checks are skipped, never replaced by a copy.
"""

import ast
import json
import os
import re
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod import crab  # noqa: E402

#: the current production CRAB client, resolved the way /cvmfs/cms.cern.ch/common/crab does
CRAB_CVMFS_BASE = "/cvmfs/cms.cern.ch/share/cms"
CRAB_CLIENT_LATEST = os.path.join(
    CRAB_CVMFS_BASE, "crab", "1.0", "etc", "crab-prod.latest"
)

#: the CMS storage descriptions Rucio's lfn2pfn reads
SITECONF = "/cvmfs/cms.cern.ch/SITECONF"

#: what CRAB's DagmanCreator appends to outLFNDirBase before resolving the write PFN:
#: <primary dataset>/<request name>/<timestamp>/<counter>
DIRECT_DEST_SUFFIX = "/CRAB_UserFiles/crab_RunProd/261015_120000/0000"

#: local account names a submitter may have; only the first is a CERN username here
LOCAL_USERS = ("kandroso", "kes", "someone")


def stageout_location(user):
    """`crab_stageout_location()` of DSProd's CRAB workflow, run under the local account `user`."""
    with mock.patch.dict(os.environ, {"USER": user, "LOGNAME": user}):
        return crab.CrabWorkflow.crab_stageout_location(None)


class ThePlaceholder(unittest.TestCase):
    def test_it_does_not_depend_on_the_local_account(self):
        for user in LOCAL_USERS:
            with self.subTest(user=user):
                self.assertEqual(
                    stageout_location(user), crab._CRAB_PLACEHOLDER_STAGEOUT
                )

    def test_it_is_a_site_and_an_lfn(self):
        site, lfn = crab._CRAB_PLACEHOLDER_STAGEOUT
        self.assertRegex(site, r"^T[0-3]_[A-Z]{2}_\w+$")
        self.assertTrue(lfn.startswith("/store/"), lfn)


class TheCrabServerRule(unittest.TestCase):
    """The placeholder against CRAB's `checkOutLFN` (skipped where /cvmfs is not mounted)."""

    @classmethod
    def setUpClass(cls):
        try:
            with open(CRAB_CLIENT_LATEST) as f:
                version = f.read().strip()
        except OSError:
            raise unittest.SkipTest(
                f"no CRAB client ({CRAB_CLIENT_LATEST}): the outLFNDirBase placeholder is "
                "not checked against CRAB's checkOutLFN"
            )
        path = os.path.join(
            CRAB_CVMFS_BASE, "crab-prod", version, "lib", "ServerUtilities.py"
        )
        with open(path) as f:
            tree = ast.parse(f.read())
        # only the function: the module's own imports need the CRAB runtime
        nodes = [
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "checkOutLFN"
        ]
        if len(nodes) != 1:
            raise AssertionError(f"{path}: no single checkOutLFN definition")
        namespace = {}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), path, "exec"), namespace)
        cls.check_out_lfn = staticmethod(namespace["checkOutLFN"])

    def test_an_lfn_naming_another_account_is_refused(self):
        # what the $USER placeholder gave a submitter whose local account is not their CERN name
        lfn = "/store/user/kes/DSProd_crab_unused"
        self.assertFalse(self.check_out_lfn(lfn, "kandroso"))
        self.assertTrue(self.check_out_lfn(lfn, "kes"))

    def test_every_submitter_may_submit_with_the_placeholder(self):
        for user in LOCAL_USERS:
            _, lfn = stageout_location(user)
            for submitter in ("kandroso", "lpcflaf", "someone"):
                with self.subTest(user=user, submitter=submitter):
                    self.assertTrue(self.check_out_lfn(lfn, submitter), lfn)


class TheSiteConfiguration(unittest.TestCase):
    """The placeholder against the site's storage.json (skipped where /cvmfs is not mounted).

    A bare prefix maps every LFN, a rule the LFNs its pattern matches, which is how the CMS
    lfn2pfn algorithm Rucio runs for the task worker starts; the real Rucio call is not made
    here, it needs a grid proxy.
    """

    @classmethod
    def setUpClass(cls):
        if not os.path.isdir(SITECONF):
            raise unittest.SkipTest(
                f"no {SITECONF}: the placeholder site is not checked against its storage rules"
            )

    @staticmethod
    def maps_for_write(site, lfn):
        with open(os.path.join(SITECONF, site, "storage.json")) as f:
            volumes = json.load(f)
        for volume in volumes:
            for protocol in volume.get("protocols", []):
                if protocol.get("access") != "global-rw":
                    continue
                if protocol.get("prefix"):
                    return True
                if any(re.match(r["lfn"], lfn) for r in protocol.get("rules", [])):
                    return True
        return False

    def test_a_site_that_maps_user_lfns_only_is_told_apart(self):
        # T3_CH_CERNBOX maps /store/user/<x> only: a /store/group placeholder there ends every
        # task SUBMITFAILED in the task worker
        dest = "/store/group/DSProd_crab_unused" + DIRECT_DEST_SUFFIX
        self.assertFalse(self.maps_for_write("T3_CH_CERNBOX", dest))
        dest = "/store/user/someone/DSProd_crab_unused" + DIRECT_DEST_SUFFIX
        self.assertTrue(self.maps_for_write("T3_CH_CERNBOX", dest))

    def test_the_placeholder_site_maps_the_placeholder_lfn(self):
        site, lfn = crab._CRAB_PLACEHOLDER_STAGEOUT
        self.assertTrue(
            self.maps_for_write(site, lfn + DIRECT_DEST_SUFFIX), (site, lfn)
        )


if __name__ == "__main__":
    unittest.main()
