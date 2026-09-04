#!/usr/bin/env python3
"""How many jobs stay in flight, and what `crab.parallel_jobs: auto` makes of it.

Nothing queues on this backend: 4798 of the 4800 branches of the Run3_2023BPix production started
within half an hour of being submitted. The makespan is therefore the ramp plus one job length per
*wave*, and the number of waves is `branches / parallel_jobs` -- which makes the ceiling the only
lever left on a production too large to go out at once. At the 87600 branches of Run3_2024 the
fixed 5000 is 17.5 waves where 8000 is 11.0, ~-37 % of the makespan; at BPix scale the two are the
same single wave, which is why `auto` is opt-in and must never resolve below the fixed default.
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
    auto_parallel_jobs,
)
from dsprod.tasks import RunProd  # noqa: E402

SETUP = "models/X_HH/setups/Run3_XHHbbWW.yaml"

#: branches of the two real productions this is measured on
BPIX_BRANCHES = 4800
Y2024_BRANCHES = 87600

#: `_CRAB_DEFAULT_PARALLEL_JOBS` and `_CRAB_AUTO_MAX_PARALLEL_JOBS`
DEFAULT_PARALLEL_JOBS = 5000
CAP = 8000

_data_dir = None
_data_before = None


def setUpModule():
    # the proxy's job manager builds a `SiteStats` under $ANALYSIS_DATA_PATH; nothing here writes
    # to it, but it must not be able to land in a production area a checkout may be driving
    global _data_dir, _data_before
    _data_before = os.environ.get("ANALYSIS_DATA_PATH")
    _data_dir = tempfile.mkdtemp(prefix="dsprod_parallel_test_")
    os.environ["ANALYSIS_DATA_PATH"] = _data_dir


def tearDownModule():
    if _data_before is None:
        os.environ.pop("ANALYSIS_DATA_PATH", None)
    else:
        os.environ["ANALYSIS_DATA_PATH"] = _data_before
    shutil.rmtree(_data_dir, ignore_errors=True)


class TestAutoSizing(unittest.TestCase):
    """`auto_parallel_jobs` on the two scales it was measured at, and at the edges."""

    def test_a_production_that_fits_one_wave_keeps_the_fixed_default(self):
        # BPix scale: the whole production is already submitted in one task, so a ceiling derived
        # from its size may not lower it -- 4800 in flight would be a *smaller* wave than today
        self.assertEqual(auto_parallel_jobs(BPIX_BRANCHES), DEFAULT_PARALLEL_JOBS)
        self.assertEqual(auto_parallel_jobs(12), DEFAULT_PARALLEL_JOBS)

    def test_it_grows_with_a_production_that_does_not(self):
        self.assertEqual(auto_parallel_jobs(6000), 6000)
        self.assertEqual(auto_parallel_jobs(CAP), CAP)

    def test_the_cap_holds_however_large_the_production(self):
        self.assertEqual(auto_parallel_jobs(Y2024_BRANCHES), CAP)
        self.assertEqual(auto_parallel_jobs(10**9), CAP)

    def test_it_never_resolves_to_laws_unlimited_sentinel(self):
        # `_set_parallel_jobs(n)` reads n <= 0 as *unlimited* (law/workflow/remote.py), which
        # submits a whole production as one CRAB task and switches the wave gate off with it
        self.assertEqual(auto_parallel_jobs(0), DEFAULT_PARALLEL_JOBS)
        self.assertEqual(auto_parallel_jobs(0, default=0, cap=0), 1)


class TestTheProxyApplies(unittest.TestCase):
    """What the real CRAB proxy ends up with, over the real setup, for each config shape."""

    def proxy(self, cfg, eras=("Run3_2023BPix",), **kwargs):
        task = RunProd(setup=SETUP, eras=eras, workflow="crab", **kwargs)
        # the checkout's own `crab:` block must not be able to move these numbers; and building
        # the job manager enters a CMSSW release, which needs a cvmfs this host may not have
        with mock.patch.object(
            CrabWorkflow, "_crab_cfg", return_value=cfg
        ), mock.patch.object(DSProdCrabJobManager, "cmssw_env", {}):
            return DSProdCrabWorkflowProxy(task=task)

    def test_no_setting_keeps_the_fixed_default(self):
        self.assertEqual(self.proxy({}).poll_data.n_parallel, DEFAULT_PARALLEL_JOBS)

    def test_auto_reads_the_branch_count_of_this_workflow(self):
        p = self.proxy({"parallel_jobs": "auto"}, eras=("Run3_2024",))
        self.assertEqual(len(p.task.get_branch_map()), Y2024_BRANCHES)
        self.assertEqual(p.poll_data.n_parallel, CAP)

    def test_auto_changes_nothing_at_the_scale_it_was_measured_against(self):
        p = self.proxy({"parallel_jobs": "auto"})
        self.assertEqual(len(p.task.get_branch_map()), BPIX_BRANCHES)
        self.assertEqual(p.poll_data.n_parallel, DEFAULT_PARALLEL_JOBS)

    def test_a_narrowed_run_is_sized_by_what_it_actually_submits(self):
        # `--points` reduces the branch map, and `auto` must follow it rather than the setup
        p = self.proxy({"parallel_jobs": "auto"}, points=("*_M-250",))
        self.assertEqual(len(p.task.get_branch_map()), 300)
        self.assertEqual(p.poll_data.n_parallel, DEFAULT_PARALLEL_JOBS)

    def test_a_configured_number_wins_over_the_sentinel_reading(self):
        self.assertEqual(self.proxy({"parallel_jobs": 250}).poll_data.n_parallel, 250)

    def test_the_command_line_wins_over_the_config(self):
        # `--parallel-jobs` is law's own parameter, already applied by its proxy; `auto` must not
        # overwrite what the operator asked for
        with mock.patch("dsprod.crab._cli_has_parallel_jobs", return_value=True):
            p = self.proxy({"parallel_jobs": "auto"}, parallel_jobs=42)
        self.assertEqual(p.poll_data.n_parallel, 42)


if __name__ == "__main__":
    unittest.main()
