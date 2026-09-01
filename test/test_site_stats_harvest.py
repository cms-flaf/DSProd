#!/usr/bin/env python3
"""What may enter a site's record, and what may not.

On 2026-09-01 the quarantine could not fire for a worker node that was failing 65 % of the jobs
sent to it, because the record said every site in the production had failed ~100 % of its jobs.
Those were not site failures: a resumed run had flipped all 8300 jobs of the previous era to
retry ("initially missing task outputs") and the harvest had counted each one against whatever
site it last ran at, so the baseline every site is compared against was itself ~100 %.
"""

import os
import sys
import tempfile
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

from dsprod.crab import DSProdCrabJobManager
from dsprod.site_stats import SiteStats


def manager(stats):
    m = DSProdCrabJobManager(sandbox_name="cmssw::CMSSW_14_0_0::arch=el9_amd64_gcc12")
    m.site_stats = stats
    return m


def job(m, num, site, status, code=None):
    """One entry of a parsed `crab status` response."""
    return m.JobId(num, "task", "/proj"), {
        "status": status,
        "code": code,
        "extra": {"site_history": ["T0_X", site]},
    }


class TestHarvestSiteStats(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.stats = SiteStats(os.path.join(self.tmp.name, "stats.json"))
        self.m = manager(self.stats)

    def tearDown(self):
        self.tmp.cleanup()

    def outcomes(self, site):
        return [ok for _, ok in self.stats.sites.get(site, {}).get("events", [])]

    def test_finished_and_failed_with_a_code_are_recorded(self):
        self.m.harvest_site_stats(
            "/proj",
            dict(
                [
                    job(self.m, 1, "T2_CH_CERN", self.m.FINISHED),
                    job(self.m, 2, "T2_EE_Estonia", self.m.FAILED, code=5),
                ]
            ),
        )
        self.assertEqual(self.outcomes("T2_CH_CERN"), [1])
        self.assertEqual(self.outcomes("T2_EE_Estonia"), [0])

    def test_a_failure_without_a_code_is_not_the_sites_doing(self):
        # a killed task reports its jobs as failed with no job-level error; counting those is
        # what let an operator's `crab kill` poison every site in the production
        self.m.harvest_site_stats(
            "/proj", dict([job(self.m, 1, "T2_CH_CERN", self.m.FAILED, code=None)])
        )
        self.assertEqual(self.outcomes("T2_CH_CERN"), [])

    def test_jobs_in_flight_are_the_denominator_not_an_outcome(self):
        self.m.harvest_site_stats(
            "/proj",
            dict(
                [
                    job(self.m, 1, "T2_CH_CERN", self.m.RUNNING),
                    job(self.m, 2, "T2_CH_CERN", self.m.PENDING),
                ]
            ),
        )
        self.assertEqual(self.outcomes("T2_CH_CERN"), [])
        self.assertEqual(self.stats.in_flight, {"T2_CH_CERN": 2})

    def test_the_same_response_twice_counts_once(self):
        result = dict([job(self.m, 1, "T2_EE_Estonia", self.m.FAILED, code=5)])
        self.m.harvest_site_stats("/proj", result)
        self.m.harvest_site_stats("/proj", result)
        self.assertEqual(self.outcomes("T2_EE_Estonia"), [0])

    def test_a_retry_that_then_succeeds_is_recorded_separately(self):
        self.m.harvest_site_stats(
            "/proj", dict([job(self.m, 1, "T2_EE_Estonia", self.m.FAILED, code=5)])
        )
        self.m.harvest_site_stats(
            "/proj", dict([job(self.m, 1, "T2_EE_Estonia", self.m.FINISHED)])
        )
        self.assertEqual(sorted(self.outcomes("T2_EE_Estonia")), [0, 1])

    def test_in_flight_is_combined_over_projects(self):
        self.m.harvest_site_stats(
            "/p1", dict([job(self.m, 1, "T1_DE_KIT", self.m.RUNNING)])
        )
        self.m.harvest_site_stats(
            "/p2", dict([job(self.m, 1, "T1_DE_KIT", self.m.RUNNING)])
        )
        self.assertEqual(self.stats.in_flight, {"T1_DE_KIT": 2})

    def test_jobs_without_a_site_history_are_skipped(self):
        jid = self.m.JobId(1, "task", "/proj")
        self.m.harvest_site_stats(
            "/proj", {jid: {"status": self.m.FINISHED, "code": 0, "extra": {}}}
        )
        self.assertEqual(self.stats.sites, {})

    def test_no_record_configured_is_a_no_op(self):
        m = manager(None)
        m.harvest_site_stats("/proj", dict([job(m, 1, "T2_CH_CERN", m.FINISHED)]))

    def test_an_unreadable_response_is_a_no_op(self):
        self.m.harvest_site_stats("/proj", None)
        self.assertEqual(self.stats.sites, {})


class TestQuarantineWithACleanBaseline(unittest.TestCase):
    """The end the harvest exists for: one bad node must stand out from healthy sites."""

    def test_a_black_hole_is_quarantined_once_the_baseline_is_real(self):
        now = 1_000_000.0
        with tempfile.TemporaryDirectory() as tmp:
            stats = SiteStats(os.path.join(tmp, "stats.json"))
            # the shape actually observed: 65 % failures at one site, a healthy pool elsewhere
            for _ in range(311):
                stats.record("T2_EE_Estonia", False, now=now)
            for _ in range(169):
                stats.record("T2_EE_Estonia", True, now=now)
            for site in ("T2_UK_London_IC", "T2_CH_CSCS", "T2_IT_Legnaro"):
                for _ in range(100):
                    stats.record(site, True, now=now)
            self.assertEqual(stats.blacklist(now=now), ["T2_EE_Estonia"])

    def test_the_poisoned_baseline_is_what_disabled_it(self):
        # every site at ~100 % failure, as the old harvest recorded after a mass retry: the
        # failing site no longer stands out and nothing is quarantined
        now = 1_000_000.0
        with tempfile.TemporaryDirectory() as tmp:
            stats = SiteStats(os.path.join(tmp, "stats.json"))
            for _ in range(311):
                stats.record("T2_EE_Estonia", False, now=now)
            for _ in range(169):
                stats.record("T2_EE_Estonia", True, now=now)
            for site in ("T2_UK_London_IC", "T2_CH_CSCS", "T2_IT_Legnaro"):
                for _ in range(100):
                    stats.record(site, False, now=now)
            self.assertEqual(stats.blacklist(now=now), [])


if __name__ == "__main__":
    unittest.main()
