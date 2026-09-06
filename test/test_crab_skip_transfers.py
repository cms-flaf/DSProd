#!/usr/bin/env python3
"""A job whose stageout is disabled must not be polled until the workflow gives up on it.

CRAB reports a job that finished its payload as `transferring`/`transferred` until it has staged
its outputs out. DSProd disables CRAB stageout entirely -- it writes every product itself -- so
those states are the end of the job. law decides that per poll from
`config.JobType.disableAutomaticOutputCollection` in the project's `crab.log`, and a log that is
missing or was rewritten without that line reads as False, at which point those jobs are polled as
running for ever. `crab_job_kwargs_query` pins it instead.
"""

import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

# `Task.to_abs` resolves the setup path against $ANALYSIS_PATH, and the checkout is the area
os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod.crab import (  # noqa: E402
    CrabWorkflow,
    DSProdCrabJobManager,
    DSProdCrabWorkflowProxy,
)
from dsprod.tasks import RunProd  # noqa: E402

SETUP = "models/X_HH/setups/Run3_XHHbbWW.yaml"
SANDBOX = "cmssw::CMSSW_14_0_0::arch=el9_amd64_gcc12"

_data_dir = None
_data_before = None


def setUpModule():
    # the proxy's job manager builds a `SiteStats` under $ANALYSIS_DATA_PATH; nothing here writes
    # to it, but it must not be able to land in a production area a checkout may be driving
    global _data_dir, _data_before
    _data_before = os.environ.get("ANALYSIS_DATA_PATH")
    _data_dir = tempfile.mkdtemp(prefix="dsprod_transfers_test_")
    os.environ["ANALYSIS_DATA_PATH"] = _data_dir


def tearDownModule():
    if _data_before is None:
        os.environ.pop("ANALYSIS_DATA_PATH", None)
    else:
        os.environ["ANALYSIS_DATA_PATH"] = _data_before
    shutil.rmtree(_data_dir, ignore_errors=True)


class TestSkipTransfersIsPinned(unittest.TestCase):
    def test_law_passes_it_to_every_status_query(self):
        task = RunProd(setup=SETUP, eras=("Run3_2023BPix",), workflow="crab")
        # building the job manager enters a CMSSW release, which needs a cvmfs this host may
        # not have; nothing about the query kwargs depends on it
        with mock.patch.object(
            CrabWorkflow, "_crab_cfg", return_value={}
        ), mock.patch.object(DSProdCrabJobManager, "cmssw_env", {}):
            proxy = DSProdCrabWorkflowProxy(task=task)
        # law's own assembly of the query kwargs, from the task attribute
        self.assertEqual(proxy._get_job_kwargs("query"), {"skip_transfers": True})

    def test_it_is_what_makes_a_transferring_job_finished(self):
        manager = DSProdCrabJobManager(sandbox_name=SANDBOX)
        for status in ("transferring", "transferred"):
            self.assertEqual(
                manager.map_status(status, skip_transfers=True), manager.FINISHED
            )
            # the reading a missing or rewritten crab.log produces
            self.assertEqual(manager.map_status(status), manager.RUNNING)


class TestWhyItCannotBeLeftToTheLog(unittest.TestCase):
    """law's fallback, exercised on the real class, on the two logs seen in a production area."""

    def test_a_project_without_a_log_says_nothing_at_all(self):
        self.assertIsNone(
            DSProdCrabJobManager._parse_log_file("/does/not/exist/crab.log")
        )

    def test_a_log_that_does_not_mention_the_setting_says_nothing_either(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "crab.log")
            with open(path, "w") as f:
                f.write(
                    "config.General.requestName = 'RunProd_Run3_XHHbbWW_deadbeef'\n"
                )
            log_data = DSProdCrabJobManager._parse_log_file(path) or {}
        self.assertIsNone(log_data.get("disable_output_collection"))


if __name__ == "__main__":
    unittest.main()
