#!/usr/bin/env python3
"""Catching a black hole in minutes rather than in hours.

The rolling-rate test is slowest against exactly the site that costs most. A black hole fails in
seconds, so it cycles through slots faster than any healthy site can finish a job, while its own
earlier successes hold its 24 h ratio below `min_failure_rate` until they age out. Measured on the
Run3_2022EE production (2026-09-13): T2_EE_Estonia's failures were visible from 07:00 -- 77 ended
failed in that hour, 287 by 08:00 -- and the site first appears in a submitted blacklist at 09:52.
Two hours, and the jobs it ate in between, because the evidence was averaged over a day.

A burst of failures in a short window is therefore enough on its own. What must not follow is the
opposite error: a fault of *ours* fails everywhere at once and looks like a burst at every site, so
the burst test keeps the same relative check the rate test has -- it fires only for a site that is
failing much harder than the others are.
"""

import os
import sys
import tempfile
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod.site_stats import DEFAULTS, SiteStats  # noqa: E402

MINUTE = 60.0
HOUR = 3600.0
BAD = "T2_XX_Blackhole"
GOOD = "T2_YY_Fine"

#: "now" of every test. Absolute, because the burst window reaches BACKWARDS: with a timeline that
#: starts at zero the window covers the whole history and no test means what it says.
NOW = 1_000_000.0


def stats(tmpdir, **cfg):
    return SiteStats(os.path.join(tmpdir, "crab_site_stats.json"), cfg or None)


def fail_fast(st, site, n, at=NOW):
    """`n` failures arriving within two minutes, as a black hole's do."""
    for i in range(n):
        st.record(site, False, now=at - 2 * MINUTE + i * (2 * MINUTE / max(n, 1)))


def recent_successes(st, site, n, minutes=20.0):
    """`n` successes spread over the last `minutes`.

    The healthy sites of a real production finish jobs continuously -- ~60 per quarter of an hour
    on the era this was measured on -- and that is what the burst test compares a suspect against.
    """
    for i in range(n):
        st.record(
            site, True, now=NOW - minutes * MINUTE + i * (minutes * MINUTE / max(n, 1))
        )


def earlier_successes(st, site, n, first_hours_ago=8.0, last_hours_ago=1.0):
    """`n` successes well before the burst window but inside the 24 h rate window."""
    span = (first_hours_ago - last_hours_ago) * HOUR
    for i in range(n):
        st.record(site, True, now=NOW - first_hours_ago * HOUR + i * span / max(n, 1))


class ABurstIsEnoughOnItsOwn(unittest.TestCase):
    def test_a_site_that_eats_a_wave_is_out_within_the_window(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            recent_successes(st, GOOD, 40)
            fail_fast(st, BAD, 25)
            self.assertEqual(st.blacklist(now=NOW), [BAD])

    def test_the_rate_test_alone_would_still_have_been_waiting(self):
        """The point of the change, pinned: the same record without the burst test is not enough.

        The site's own successes earlier in the day keep its 24 h ratio under `min_failure_rate`.
        """
        with tempfile.TemporaryDirectory() as d:
            st = stats(d, burst_failures=10**6)  # burst effectively disabled
            recent_successes(st, GOOD, 40)
            earlier_successes(st, BAD, 40)
            fail_fast(st, BAD, 25)
            # 25 failures against 65 outcomes over the day is 38 %, under `min_failure_rate`
            self.assertEqual(st.blacklist(now=NOW), [])

    def test_and_with_the_burst_test_the_same_record_is_caught(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            recent_successes(st, GOOD, 40)
            earlier_successes(st, BAD, 40)
            fail_fast(st, BAD, 25)
            self.assertEqual(st.blacklist(now=NOW), [BAD])

    def test_the_defaults_are_the_documented_ones(self):
        self.assertEqual(DEFAULTS["burst_failures"], 20)
        self.assertEqual(DEFAULTS["burst_minutes"], 15.0)


class AndOnlyWhenItIsReallyABurst(unittest.TestCase):
    def test_the_same_failures_spread_over_a_day_are_not_one(self):
        """Enough successes that the rate test stays quiet, so only a burst could fire."""
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            earlier_successes(st, GOOD, 40)
            earlier_successes(st, BAD, 60)
            for i in range(
                25
            ):  # one failure every 40 min: never 20 within a quarter hour
                st.record(BAD, False, now=NOW - i * 40 * MINUTE)
            self.assertEqual(st.blacklist(now=NOW), [])

    def test_too_few_failures_to_be_one(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            recent_successes(st, GOOD, 40)
            earlier_successes(st, BAD, 60)
            fail_fast(st, BAD, 19)
            self.assertEqual(st.blacklist(now=NOW), [])

    def test_jobs_still_running_are_not_what_a_burst_is_measured_against(self):
        """The denominator the two tests do not share, pinned.

        The rolling rate counts every job SENT to a site, in flight included; the burst counts only
        what ENDED inside its window, because a job still running carries no timestamp that could
        place it in a quarter of an hour. So a large site whose thousand running jobs are perfectly
        healthy is still caught on the failures one bad node produced in ten minutes -- under the
        other denominator, 25 failures among 1025 would be a 2 % rate and nothing would fire.
        """
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            st.set_in_flight({BAD: 1000})
            recent_successes(st, GOOD, 40)
            fail_fast(st, BAD, 25)
            self.assertEqual(st.blacklist(now=NOW), [BAD])
        with tempfile.TemporaryDirectory() as d:
            # and it really is the burst that fires: the rate test sees the 2 % and stays quiet
            st = stats(d, burst_failures=10**6)
            st.set_in_flight({BAD: 1000})
            recent_successes(st, GOOD, 40)
            fail_fast(st, BAD, 25)
            self.assertEqual(st.blacklist(now=NOW), [])

    def test_a_busy_site_that_mostly_succeeds_is_left_alone(self):
        """20 failures among 200 outcomes in the window is a 10 % rate, not a black hole."""
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            recent_successes(st, GOOD, 40)
            fail_fast(st, BAD, 20)
            # 180 successes inside the same window: a 10 % failure rate, not a black hole
            for i in range(180):
                st.record(BAD, True, now=NOW - 10 * MINUTE + i * (10 * MINUTE / 180))
            self.assertEqual(st.blacklist(now=NOW), [])


class OurOwnBugMustNotBanTheGrid(unittest.TestCase):
    """The adversarial case: a payload fault fails everywhere, and looks like a burst everywhere."""

    def test_a_failure_that_hits_every_site_quarantines_none_of_them(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            for site in ("T2_A_A", "T2_B_B", "T2_C_C", "T2_D_D"):
                fail_fast(st, site, 30)
            self.assertEqual(st.blacklist(now=NOW), [])

    def test_but_the_one_site_failing_far_harder_than_the_rest_is(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            # the others fail some of the time; the black hole fails all of it
            for site in ("T2_A_A", "T2_B_B", "T2_C_C"):
                fail_fast(st, site, 4)
                for i in range(36):
                    st.record(
                        site, True, now=NOW - 10 * MINUTE + i * (10 * MINUTE / 36)
                    )
            fail_fast(st, BAD, 25)
            self.assertEqual(st.blacklist(now=NOW), [BAD])

    def test_a_site_is_not_its_own_baseline(self):
        """A black hole large enough to dominate the grid must not be compared against itself.

        Its own failures would then set the bar it is measured against, and the worse it got the
        more normal it would look -- the failure mode the rolling rate test was already built to
        avoid.
        """
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            earlier_successes(st, BAD, 400)
            fail_fast(st, BAD, 300)
            recent_successes(st, GOOD, 30, minutes=10.0)
            self.assertEqual(st.blacklist(now=NOW), [BAD])

    def test_with_nothing_to_compare_against_nothing_is_quarantined(self):
        """A single site in the record has no baseline, so it can never be judged worse."""
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            fail_fast(st, BAD, 50)
            self.assertEqual(st.blacklist(now=NOW), [])

    def test_a_quiet_grid_leaves_the_burst_test_without_a_comparison(self):
        """The limit of the burst test, stated rather than discovered: it needs other sites to
        have ENDED jobs in the same window. Early in a wave, when nothing has finished anywhere,
        only the standing rate test can fire -- which is the conservative direction."""
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            earlier_successes(
                st, GOOD, 40
            )  # all of it hours ago, none inside the window
            earlier_successes(st, BAD, 40)
            fail_fast(st, BAD, 25)
            self.assertEqual(st.blacklist(now=NOW), [])


class TheBurstTestRespectsTheRestOfTheMachinery(unittest.TestCase):
    def test_a_lifted_quarantine_is_not_re_armed_by_the_burst_that_earned_it(self):
        """`cleared_at` bounds the burst window too, or a served ban would re-arm immediately."""
        with tempfile.TemporaryDirectory() as d:
            # a ban SHORTER than the burst window, or the failures that earned it age out of the
            # window on their own and the clamp is never what keeps the site out of a second one
            st = stats(d, quarantine_hours=0.1)
            recent_successes(st, GOOD, 40, minutes=8.0)
            fail_fast(st, BAD, 25)
            self.assertEqual(st.blacklist(now=NOW), [BAD])
            lifted = st.sites[BAD]["quarantined_until"] + 1.0
            self.assertEqual(st.blacklist(now=lifted), [])

    def test_a_burst_quarantine_escalates_like_any_other(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d, quarantine_hours=1.0)
            earlier_successes(st, GOOD, 40)
            fail_fast(st, BAD, 25)
            st.blacklist(now=NOW)
            first = st.sites[BAD]["quarantined_until"] - NOW
            lifted = st.sites[BAD]["quarantined_until"] + 1.0
            st.blacklist(now=lifted)
            for i in range(40):
                st.record(GOOD, True, now=lifted + i)
            for i in range(25):
                st.record(BAD, False, now=lifted + 60 + i)
            st.blacklist(now=lifted + 3 * MINUTE)
            second = st.sites[BAD]["quarantined_until"] - (lifted + 3 * MINUTE)
            self.assertAlmostEqual(second / first, 2.0, places=1)

    def test_a_disabled_record_still_quarantines_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d, enabled=False)
            earlier_successes(st, GOOD, 40)
            fail_fast(st, BAD, 50)
            self.assertEqual(st.blacklist(now=NOW), [])


if __name__ == "__main__":
    unittest.main()
