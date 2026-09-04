#!/usr/bin/env python3
"""Two ways the CRAB path spent a production's wall clock, measured on Run3_2023BPix.

That production ran 4800 `RunProd` branches and took 68.4 h to reach 99.4 %. Retries held back
by the wave gate waited 11.35 h at the median -- roughly one job length (7.1 h at the median) per
retry generation, ~10.5 h of the total -- because the gate weighed a handful of retries against a
wave they could never fill. And with law's defaults (`retries` 5, `tolerance` 0.0) the first
branch to burn its attempts raises `tolerance exceeded` and takes the run down with 4799 finished
jobs, which a release window makes easy to reach: 56 % of failures arrive in under 6 min.
"""

import os
import shutil
import sys
import tempfile
import time
import unittest
from collections import OrderedDict
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

# `Task.to_abs` resolves the setup path against $ANALYSIS_PATH, and the checkout is the area
os.environ["ANALYSIS_PATH"] = dsprod_repo

import luigi  # noqa: E402

from dsprod.crab import DSProdCrabWorkflowProxy, _CrabProxyBase  # noqa: E402
from dsprod.tasks import NanoMergeTask, RunProd  # noqa: E402

SETUP = "models/X_HH/setups/Run3_XHHbbWW.yaml"

#: the production the numbers in the module docstring come from
N_BRANCHES = 4800
#: `_CRAB_DEFAULT_PARALLEL_JOBS`, and `refill_fraction` 0.2 of it: a wave needs 1000 jobs
PARALLEL_JOBS = 5000
MIN_WAVE = 1000

_data_dir = None


def setUpModule():
    # nothing here writes job data (`dump_job_data` is replaced below), but a stray write must
    # never land in the production area a checkout may be driving
    global _data_dir
    _data_dir = tempfile.mkdtemp(prefix="dsprod_test_")
    os.environ["ANALYSIS_DATA_PATH"] = _data_dir


def tearDownModule():
    shutil.rmtree(_data_dir, ignore_errors=True)


def task(**kwargs):
    """One point of one era of the real setup, submitted through CRAB."""
    kwargs.setdefault("workflow", "crab")
    return RunProd(setup=SETUP, eras=("Run3_2023BPix",), points=("*_M-250",), **kwargs)


def proxy(t=None):
    """A fresh proxy over the real task, so no test inherits another's poll or job data."""
    p = DSProdCrabWorkflowProxy(task=t or task())
    p.poll_data.n_parallel = PARALLEL_JOBS
    return p


def jobs(*job_nums):
    return OrderedDict((job_num, [job_num]) for job_num in job_nums)


class TestWaveGate(unittest.TestCase):
    """The decision table of `_should_submit_crab_group`, at a 1000-job wave in 5000 slots."""

    def setUp(self):
        self.proxy = proxy()

    def decide(self, n_backlog, n_retry, n_active, parked_min_ago=None):
        self.proxy.poll_data.n_active = n_active
        self.proxy._retry_parked_since = (
            None if parked_min_ago is None else time.time() - parked_min_ago * 60
        )
        return self.proxy._should_submit_crab_group(n_backlog, n_retry)

    def test_nothing_waiting_is_not_held_back(self):
        self.assertTrue(self.decide(n_backlog=0, n_retry=0, n_active=N_BRANCHES))

    def test_a_full_wave_of_backlog_with_room_goes_out(self):
        self.assertTrue(self.decide(n_backlog=N_BRANCHES, n_retry=0, n_active=0))
        self.assertTrue(self.decide(n_backlog=MIN_WAVE, n_retry=0, n_active=0))

    def test_a_full_wave_without_room_waits(self):
        # 500 free slots cannot take a 1000-job wave, and the running jobs will free more
        self.assertFalse(self.decide(n_backlog=MIN_WAVE, n_retry=0, n_active=4500))

    def test_a_handful_of_retries_is_parked(self):
        # the incident: gating on free slots alone opened the gate from the first poll, because
        # 3270 of 5000 slots taken leaves 1730 free -- one CRAB task per retry generation
        self.assertFalse(self.decide(n_backlog=0, n_retry=5, n_active=3270))

    def test_a_wave_that_can_no_longer_be_filled_goes_out_at_once(self):
        # the tail: 5 retries and 100 jobs still running can never reach 1000, so waiting for a
        # wave would only delay them -- this is what keeps a large production's tail short
        self.assertTrue(self.decide(n_backlog=0, n_retry=5, n_active=100))

    def test_a_small_production_is_never_batched(self):
        # 12 jobs can never fill a wave, so a failure there is resubmitted on the next poll
        self.assertTrue(self.decide(n_backlog=12, n_retry=0, n_active=0))
        self.assertTrue(self.decide(n_backlog=0, n_retry=1, n_active=11))

    def test_unlimited_parallelism_keeps_laws_behaviour(self):
        self.proxy.poll_data.n_parallel = self.proxy.n_parallel_max
        self.assertTrue(self.decide(n_backlog=0, n_retry=1, n_active=N_BRANCHES - 1))

    def test_a_parked_retry_waits_out_its_window(self):
        self.assertFalse(
            self.decide(n_backlog=5, n_retry=0, n_active=4795, parked_min_ago=10)
        )
        self.assertFalse(
            self.decide(n_backlog=5, n_retry=0, n_active=4795, parked_min_ago=44)
        )

    def test_a_parked_retry_is_released_when_the_window_is_up(self):
        # what the 11.35 h median parking cost: without this the 5 retries wait for a wave of
        # 1000 that only the next era could ever bring
        self.assertTrue(
            self.decide(n_backlog=5, n_retry=0, n_active=4795, parked_min_ago=46)
        )

    def test_the_timer_only_runs_for_parked_work(self):
        # a retry offered this poll has waited for nothing yet, however long the driver has run
        self.assertFalse(self.decide(n_backlog=0, n_retry=5, n_active=4795))

    def test_the_release_window_is_configurable(self):
        with mock.patch.object(
            self.proxy.task, "_crab_cfg", return_value={"retry_release_minutes": 5}
        ):
            self.assertTrue(
                self.decide(n_backlog=5, n_retry=0, n_active=4795, parked_min_ago=6)
            )
            self.assertFalse(
                self.decide(n_backlog=5, n_retry=0, n_active=4795, parked_min_ago=4)
            )

    def test_an_unreadable_release_window_falls_back_to_the_default(self):
        with mock.patch.object(
            self.proxy.task, "_crab_cfg", return_value={"retry_release_minutes": "soon"}
        ):
            self.assertEqual(self.proxy._crab_retry_release_minutes(), 45.0)

    def test_the_size_bar_ignores_the_retries_offered_this_poll(self):
        # a whole generation of retries is parked first and goes out on the next poll as backlog,
        # one poll interval later, rather than opening the gate before it has ever waited
        self.assertFalse(self.decide(n_backlog=0, n_retry=MIN_WAVE, n_active=3800))
        self.assertTrue(self.decide(n_backlog=MIN_WAVE, n_retry=0, n_active=3800))


class TestRetryParking(unittest.TestCase):
    """Where a parked retry lives, in what order, and what a killed driver finds."""

    def setUp(self):
        self.proxy = proxy()
        # nothing has been produced yet, so no job is skippable and no storage is consulted
        self.proxy._existing_branches = set()
        self.dumped = []
        self.proxy.dump_job_data = lambda: self.dumped.append(
            list(self.proxy.job_data.unsubmitted_jobs)
        )

    def park(self, *job_nums, n_active=None):
        for job_num in job_nums:
            self.proxy.job_data.jobs[job_num] = self.proxy.job_data_cls.job_data(
                branches=[job_num]
            )
        self.proxy.poll_data.n_active = (
            N_BRANCHES - len(job_nums) if n_active is None else n_active
        )
        return self.proxy.submit(jobs(*job_nums))

    def test_parked_retries_stay_in_the_job_data(self):
        # a proxy-side dict would orphan them on a driver kill and, since law snapshots
        # `len(job_data)` as the poll loop's n_jobs, drop them out of the production's total
        submitted = self.park(1, 2, 3)
        self.assertEqual(dict(submitted), {})
        self.assertEqual(list(self.proxy.job_data.unsubmitted_jobs), [1, 2, 3])
        self.assertEqual(dict(self.proxy.job_data.jobs), {})
        self.assertEqual(len(self.proxy.job_data), 3)
        self.assertEqual(
            self.dumped, [[1, 2, 3]], "a killed driver reads the dump, not memory"
        )

    def test_parked_retries_go_in_front_of_the_backlog(self):
        # law's submit() fills the next wave from `unsubmitted_jobs` in dict order, so a retry
        # appended behind a large backlog is not reached and the release timer cannot free it
        self.proxy.job_data.unsubmitted_jobs.update({n: [n] for n in range(100, 140)})
        self.park(1, 2, n_active=N_BRANCHES - 42)
        self.assertEqual(list(self.proxy.job_data.unsubmitted_jobs)[:2], [1, 2])
        self.assertEqual(len(self.proxy.job_data.unsubmitted_jobs), 42)

    def test_the_clock_starts_with_the_oldest_parked_retry(self):
        self.park(1, 2)
        first = self.proxy._retry_parked_since = (
            self.proxy._retry_parked_since - 30 * 60
        )
        self.park(3)
        self.assertEqual(self.proxy._retry_parked_since, first)

    def test_a_release_delegates_to_law_and_clears_the_clock(self):
        self.proxy.poll_data.n_active = N_BRANCHES - 1
        self.proxy._retry_parked_since = time.time() - 46 * 60
        with mock.patch.object(
            _CrabProxyBase, "submit", autospec=True, return_value=OrderedDict()
        ) as submit:
            self.proxy.submit(jobs(4))
        self.assertEqual(submit.call_count, 1)
        self.assertEqual(list(submit.call_args[0][1]), [4])
        self.assertIsNone(self.proxy._retry_parked_since)

    def test_a_backlog_wave_clears_the_clock_too(self):
        # law's submit() drains `unsubmitted_jobs` up to n_parallel whatever opened the gate, so
        # the parked retries left with this wave -- and whatever did not has no free slot anyway.
        # A clock left running here would open the gate on every poll for the rest of the run.
        self.park(1, 2)
        self.proxy.job_data.unsubmitted_jobs.update({n: [n] for n in range(100, 1100)})
        self.proxy.poll_data.n_active = 0
        with mock.patch.object(
            _CrabProxyBase, "submit", autospec=True, return_value=OrderedDict()
        ) as submit:
            self.proxy.submit()
        self.assertEqual(submit.call_count, 1)
        self.assertIsNone(self.proxy._retry_parked_since)


class TestFailureBudget(unittest.TestCase):
    """`RunProd`'s retry and failure budget, and that nothing on the way to it wins."""

    def setUp(self):
        self.merge = NanoMergeTask(
            setup=SETUP, eras=("Run3_2023BPix",), points=("*_M-250",)
        )
        self.direct = task(workflow="htcondor")

    def test_one_dead_branch_no_longer_kills_the_production(self):
        # law: n_failed_max = tolerance * n_jobs, and it raises `tolerance exceeded` above it
        n_failed_max = self.direct.tolerance * N_BRANCHES
        self.assertGreaterEqual(
            n_failed_max, 1.0, "one branch out of attempts must not end the run"
        )
        self.assertLess(
            n_failed_max,
            N_BRANCHES / 10.0,
            "a production that fails everywhere must still stop early",
        )

    def test_acceptance_still_forbids_a_short_sample(self):
        # tolerance keeps the poll loop alive; acceptance is what makes the workflow fail once no
        # job is left to finish ("acceptance of N not reached"), so the sample is never silently
        # short. law only stops early on an unreachable acceptance if asked to, and is not.
        self.assertEqual(self.direct.acceptance, 1.0)
        self.assertFalse(self.direct.check_unreachable_acceptance)

    def test_the_retry_budget_is_bounded_but_not_one_shot(self):
        attempts = (
            self.direct.retries + 1
        )  # law submits a job once, then retries it `retries`
        self.assertGreater(
            attempts, 1, "one transient site failure must not condemn a branch"
        )
        self.assertLessEqual(
            attempts * 7.1,
            30.0,
            "a branch that keeps dying must not burn days of wall clock",
        )

    def test_the_merge_task_no_longer_overwrites_the_budget(self):
        # the leak: driving a production through NanoMergeTask handed `RunProd` the merge task's
        # own law defaults, so the production ran with tolerance 0.0 whatever RunProd declares
        leaked = RunProd.req_params(self.merge, _skip_task_excludes=True)
        self.assertEqual(leaked["tolerance"], 0.0)
        self.assertNotEqual(leaked["tolerance"], self.direct.tolerance)

        params = RunProd.req_params(self.merge)
        self.assertNotIn("tolerance", params)
        self.assertNotIn("retries", params)
        required = RunProd.req(self.merge)
        self.assertEqual(required.tolerance, self.direct.tolerance)
        self.assertEqual(required.retries, self.direct.retries)

    def test_a_command_line_budget_survives_the_merge_task(self):
        # `--RunProd-tolerance 0.5` arrives as a luigi class-level value; without the exclusion
        # `req()` would pass the merge task's own value on top of it and discard it silently
        cfg = luigi.configuration.get_config()
        cfg.set("RunProd", "retries", "9")
        cfg.set("RunProd", "tolerance", "0.5")
        try:
            self.assertEqual(RunProd.req(self.merge).retries, 9)
            self.assertEqual(RunProd.req(self.merge).tolerance, 0.5)
        finally:
            cfg.remove_option("RunProd", "retries")
            cfg.remove_option("RunProd", "tolerance")

    def test_the_budget_is_still_a_command_line_parameter(self):
        # a plain class attribute would replace the luigi Parameter and lose --RunProd-retries
        params = dict(RunProd.get_params())
        self.assertIsInstance(params["retries"], luigi.IntParameter)
        self.assertIsInstance(params["tolerance"], luigi.FloatParameter)
        # and both must stay insignificant: a budget is not part of the task id, so changing one
        # must not move a single output path
        self.assertFalse(params["retries"].significant)
        self.assertFalse(params["tolerance"].significant)


if __name__ == "__main__":
    unittest.main()
