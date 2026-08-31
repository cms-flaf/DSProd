#!/usr/bin/env python3
"""Each test here is a failure that stopped a real production.

On 2026-08-31 a Run3_2023 production died mid-merge and the restart resubmitted all 8300
`RunProd` jobs of the era, because `NanoMergeTask` had merged their nano files and deleted
them -- exactly what it is supposed to do -- and law reads a missing output as work to redo.
The same restart had been triggered by a transient EOS failure that made law build the path
of its own CRAB wrapper as `.../job.py/crab/crab_wrapper.sh`.
"""

import os
import sys
import types
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

import law.workflow.remote

from dsprod.crab import _strict_rel_path
from dsprod.tasks import merge_groups
from dsprod.tools import StopOnMassInitialRetryProxy


class TestStrictRelPath(unittest.TestCase):
    """A module file must never be taken for a directory, stat or no stat."""

    def test_module_anchor_resolves_without_stat(self):
        # the incident: os.path.exists() on the EOS-hosted law tree returned False, and law's
        # own rel_path then kept the file name, yielding .../job.py/crab/crab_wrapper.sh
        anchor = "/eos/nowhere/site-packages/law/contrib/cms/job.py"
        self.assertFalse(os.path.exists(anchor), "the anchor must be unstattable")
        self.assertEqual(
            _strict_rel_path(anchor, "crab", "crab_wrapper.sh"),
            "/eos/nowhere/site-packages/law/contrib/cms/crab/crab_wrapper.sh",
        )

    def test_existing_file_anchor_resolves(self):
        self.assertEqual(
            _strict_rel_path(__file__, "sibling.txt"),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "sibling.txt"),
        )

    def test_directory_anchor_is_kept(self):
        self.assertEqual(
            _strict_rel_path(dsprod_repo, "dsprod", "tasks.py"),
            os.path.join(dsprod_repo, "dsprod", "tasks.py"),
        )


class TestMergeGroups(unittest.TestCase):
    """The grouping the backfill trusts to say which seeds a merged file accounts for."""

    def test_exact_multiple(self):
        groups = merge_groups(list(range(1, 101)), 50)
        self.assertEqual([g for g, _ in groups], [0, 1])
        self.assertEqual(groups[0][1][0], 1)
        self.assertEqual(groups[1][1][-1], 100)

    def test_partial_last_group(self):
        groups = merge_groups(list(range(1, 12)), 5)
        self.assertEqual([len(s) for _, s in groups], [5, 5, 1])

    def test_every_seed_covered_exactly_once(self):
        seeds = list(range(1, 8301))
        covered = [s for _, group in merge_groups(seeds, 50) for s in group]
        self.assertEqual(covered, seeds)

    def test_group_count_matches_the_production(self):
        # 8300 seeds at 50 per merge = 166 groups per nano version, 332 over v12 + v15
        self.assertEqual(len(merge_groups(list(range(1, 8301)), 50)), 166)


class TestBackfillNames(unittest.TestCase):
    """The backfill decides from directory listings, so a missing directory must not raise."""

    def names(self, target):
        from dsprod.tasks import BackfillProducedRecords

        return BackfillProducedRecords._names(target)

    def test_listing_is_returned_as_a_set(self):
        target = types.SimpleNamespace(listdir=lambda: ["a.json", "b.json"])
        self.assertEqual(self.names(target), {"a.json", "b.json"})

    def test_missing_directory_is_empty_not_fatal(self):
        # nothing has been produced for this point yet, so the directory does not exist
        def boom():
            raise RuntimeError("No such file or directory")

        self.assertEqual(self.names(types.SimpleNamespace(listdir=boom)), set())


class _Proxy(StopOnMassInitialRetryProxy):
    """The mixin over the real `JobData`, since only the proxy needs a scheduled workflow.

    Hand-mirroring law's job bookkeeping is what makes a test double drift: the length that
    the brake divides by is `len(jobs) + len(unsubmitted_jobs)`, and that belongs to law.
    """

    def __init__(self, jobs, unsubmitted=0, submitted=True):
        self._submitted = submitted
        self.job_data = law.workflow.remote.JobData()
        self.job_data.jobs.update(jobs)
        for i in range(unsubmitted):
            self.job_data.unsubmitted_jobs[10**6 + i] = [10**6 + i]


def missing_outputs_jobs(n, first=1):
    return {job_num: [job_num - 1] for job_num in range(first, first + n)}


def job_data(n_missing, n_ok):
    jobs = {}
    for job_num in range(1, n_missing + 1):
        jobs[job_num] = {
            "status": "finished",
            "error": StopOnMassInitialRetryProxy.missing_outputs_error,
        }
    for job_num in range(n_missing + 1, n_missing + n_ok + 1):
        jobs[job_num] = {"status": "finished", "error": None}
    return jobs


class TestMassInitialRetryBrake(unittest.TestCase):
    """A resumed run must not regenerate a production whose outputs were consumed."""

    def test_the_incident_is_refused(self):
        # 8299 of 8300 jobs recorded finished lost their outputs to the merge
        proxy = _Proxy(job_data(8299, 1))
        with self.assertRaises(Exception) as ctx:
            proxy.stop_on_mass_initial_retry(missing_outputs_jobs(8299))
        msg = str(ctx.exception)
        self.assertIn("8299 of 8300", msg)
        self.assertIn("produced/", msg, "the message must say what marks a seed done")
        self.assertIn("data/jobs/", msg, "and how to force a deliberate redo")

    def test_a_handful_of_lost_outputs_still_retries(self):
        # someone deleted a few files by hand: that is what the retry is for
        proxy = _Proxy(job_data(5, 8295))
        proxy.stop_on_mass_initial_retry(missing_outputs_jobs(5))

    def test_boundary_at_the_fraction(self):
        n = 8300
        allowed = int(StopOnMassInitialRetryProxy.max_initial_retry_fraction * n)
        proxy = _Proxy(job_data(allowed, n - allowed))
        proxy.stop_on_mass_initial_retry(missing_outputs_jobs(allowed))
        proxy = _Proxy(job_data(allowed + 1, n - allowed - 1))
        with self.assertRaises(Exception):
            proxy.stop_on_mass_initial_retry(missing_outputs_jobs(allowed + 1))

    def test_single_job_workflow_is_never_blocked(self):
        # a one-job workflow is 100 % of itself; the brake must not deadlock it
        proxy = _Proxy(job_data(1, 0))
        proxy.stop_on_mass_initial_retry(missing_outputs_jobs(1))

    def test_fresh_run_is_not_affected(self):
        proxy = _Proxy(job_data(8299, 1), submitted=False)
        proxy.stop_on_mass_initial_retry(missing_outputs_jobs(8299))

    def test_no_retries_is_not_affected(self):
        proxy = _Proxy(job_data(8299, 1))
        proxy.stop_on_mass_initial_retry(None)
        proxy.stop_on_mass_initial_retry({})

    def test_retries_for_other_reasons_pass_through(self):
        # jobs that actually failed on the grid must keep being resubmitted, however many
        jobs = {
            job_num: {"status": "failed", "error": "exit code 84"}
            for job_num in range(1, 8301)
        }
        proxy = _Proxy(jobs)
        proxy.stop_on_mass_initial_retry(missing_outputs_jobs(8300))

    def test_unsubmitted_jobs_count_towards_the_total(self):
        # law's JobData length is jobs + unsubmitted: 400 of 8300 must pass, not 400 of 400
        proxy = _Proxy(job_data(400, 0), unsubmitted=7900)
        proxy.stop_on_mass_initial_retry(missing_outputs_jobs(400))


if __name__ == "__main__":
    unittest.main()
