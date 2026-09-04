#!/usr/bin/env python3
"""A step must deliver exactly the events it was asked for.

`-n <n>` is a request. A step that returns fewer produces a perfectly valid file, every later
step carries the shortfall forward, and the only downstream check is that a merged file holds the
sum of its own inputs -- as true of short ones as of full ones. One Run3_2023 job returned 999 of
its 1000 events and the merged file was delivered holding 49 999.
"""

import os
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

# `_cmsenv_prefix` reads it when a step is run; nothing here reaches a release
os.environ.setdefault("ANALYSIS_PATH", dsprod_repo)

from dsprod import run_step  # noqa: E402

PARAMS = {"CMSSW": "CMSSW_13_0_13", "SCRAM_ARCH": "el8_amd64_gcc11"}


class TestAssertStepEvents(unittest.TestCase):
    def counted(self, n):
        return mock.patch.object(run_step, "count_events", return_value=[n])

    def test_the_exact_count_passes(self):
        with self.counted(1000):
            run_step.assert_step_events("NANO", PARAMS, "/w", "NANO_v12.root", 1000)

    def test_one_event_short_is_refused(self):
        # the case that was delivered: 999 of 1000
        with self.counted(999):
            with self.assertRaises(RuntimeError) as ctx:
                run_step.assert_step_events("LHEGS", PARAMS, "/w", "LHEGS.root", 1000)
        msg = str(ctx.exception)
        self.assertIn("LHEGS", msg, "the message must name the step that came up short")
        self.assertIn("999", msg)
        self.assertIn("1000", msg)

    def test_more_events_than_asked_is_refused_too(self):
        # a step that returns more means the request was not what ran, which is equally wrong
        with self.counted(1001):
            with self.assertRaises(RuntimeError):
                run_step.assert_step_events("NANO", PARAMS, "/w", "NANO_v12.root", 1000)

    def test_the_output_is_counted_in_its_own_release(self):
        # each step has its own CMSSW, and the file must be counted in the one that wrote it
        with mock.patch.object(
            run_step, "count_events", return_value=[1000]
        ) as counted:
            run_step.assert_step_events("RECO", PARAMS, "/work", "RECO.root", 1000)
        params, paths, work_dir = counted.call_args[0]
        self.assertIs(params, PARAMS)
        self.assertEqual(paths, [os.path.join("/work", "RECO.root")])
        self.assertEqual(work_dir, "/work")

    def test_a_test_run_contracts_its_own_short_count(self):
        # `--test 5` asks each step for 5 events, so 5 is exact and 1000 would be wrong
        with self.counted(5):
            run_step.assert_step_events("NANO", PARAMS, "/w", "NANO_v12.root", 5)


class TestRunStepChecksItsOutput(unittest.TestCase):
    """Every step goes through `run_step`, so the check cannot be bypassed by a chain."""

    def run_one(self, produced, n_evt=1000, fileout="GEN-SIM.root"):
        with mock.patch.object(
            run_step, "build_cmsdriver", return_value="cmsDriver.py"
        ), mock.patch("dsprod.tools.ps_call"), mock.patch.object(
            run_step, "count_events", return_value=[produced]
        ) as counted:
            run_step.run_step(
                "GEN-SIM", PARAMS, "/w", seed=1, n_evt=n_evt, fileout=fileout, verbose=0
            )
        return counted

    def test_a_step_that_delivers_is_accepted(self):
        self.assertTrue(self.run_one(1000).called)

    def test_a_step_that_comes_up_short_stops_the_job(self):
        with self.assertRaises(RuntimeError):
            self.run_one(998)

    def test_a_step_with_no_output_file_is_not_counted(self):
        # nothing to count, and asking would enter the release for no reason
        self.assertFalse(self.run_one(1000, fileout=None).called)

    def test_a_step_asked_for_no_events_is_not_counted(self):
        self.assertFalse(self.run_one(1000, n_evt=0).called)


if __name__ == "__main__":
    unittest.main()
