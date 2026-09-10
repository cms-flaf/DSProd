#!/usr/bin/env python3
"""What happens to a site that is still broken when its quarantine runs out.

A fixed 6-hour ban that also wiped the site's record was no defence against a site that stays
broken: it returned to the whitelist with a clean sheet, had to earn `min_failures` all over again
-- five more dead jobs, each costing a branch one of its attempts -- and bought itself another wave
every six hours. Measured on the Run3_2023BPix production: T2_EE_Estonia (1984/2030 failed),
T2_BE_IIHE (1219/1278) and T2_TR_METU (711/765) each cycled in and out three times over
2026-09-08..10, and 93 % of the production's 4197 job failures came from those three.

So the ban now starts at a day and doubles per offence up to 32 days, and the count survives the
ban. Keeping the record creates one hazard of its own, which the last class here pins: the evidence
a ban was served for must not immediately earn the next one, or a site would never get the second
chance the escalation assumes it had.
"""

import json
import os
import sys
import tempfile
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod.site_stats import DEFAULTS, SiteStats  # noqa: E402

HOUR = 3600.0
BAD = "T2_XX_Broken"
GOOD = "T2_YY_Fine"


def stats(tmpdir, **cfg):
    return SiteStats(os.path.join(tmpdir, "crab_site_stats.json"), cfg or None)


def make_it_fail(st, now, n=6, site=BAD):
    """Enough failures at `site`, against a real baseline elsewhere, to earn a quarantine."""
    for i in range(n):
        st.record(site, False, now=now + i)
    for i in range(40):
        st.record(GOOD, True, now=now + i)


def quarantine_span(st, site, now):
    """Hours the current quarantine of `site` still has to run."""
    return (st.sites[site]["quarantined_until"] - now) / HOUR


class TheFirstQuarantine(unittest.TestCase):
    def test_it_lasts_the_configured_base(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            make_it_fail(st, 0.0)
            self.assertEqual(st.blacklist(now=100.0), [BAD])
            self.assertAlmostEqual(quarantine_span(st, BAD, 100.0), 24.0, places=3)

    def test_the_default_is_a_day_not_six_hours(self):
        """The value itself, since it is the one an operator reads out of the docs."""
        self.assertEqual(DEFAULTS["quarantine_hours"], 24.0)
        self.assertEqual(DEFAULTS["max_quarantine_hours"], 768.0)


class EachFurtherOneDoubles(unittest.TestCase):
    """The point of the change: a site that keeps failing is held out for longer each time."""

    def serve_and_reoffend(self, st, rounds, start=0.0):
        """Earn a quarantine, sit it out, fail again -- `rounds` times. Returns the ban lengths."""
        spans, now = [], start
        for _ in range(rounds):
            make_it_fail(st, now)
            self.assertEqual(st.blacklist(now=now + 100.0), [BAD])
            spans.append(quarantine_span(st, BAD, now + 100.0))
            # let the ban run out, then the site fails again on fresh work
            now = st.sites[BAD]["quarantined_until"] + 1.0
            self.assertEqual(st.blacklist(now=now), [])
        return spans

    def test_the_lengths_double(self):
        with tempfile.TemporaryDirectory() as d:
            spans = self.serve_and_reoffend(stats(d), 4)
        self.assertEqual([round(s) for s in spans], [24, 48, 96, 192])

    def test_the_doubling_stops_at_32_days(self):
        with tempfile.TemporaryDirectory() as d:
            spans = self.serve_and_reoffend(stats(d), 8)
        self.assertEqual(
            [round(s) for s in spans], [24, 48, 96, 192, 384, 768, 768, 768]
        )
        self.assertEqual(round(max(spans) / 24), 32, "the cap is 32 days")

    def test_the_cap_is_configurable_and_respected(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d, quarantine_hours=1.0, max_quarantine_hours=2.0)
            spans = self.serve_and_reoffend(st, 4)
        self.assertEqual([round(s) for s in spans], [1, 2, 2, 2])

    def test_polling_through_an_active_ban_does_not_escalate_it(self):
        """The one arithmetic hazard of the design: `blacklist()` runs `_quarantine` on every
        submission, and a ban lasts many polls. Escalating per poll rather than per offence would
        turn one bad wave into 24 -> 48 -> 96 h within three polls."""
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            make_it_fail(st, 0.0)
            self.assertEqual(st.blacklist(now=100.0), [BAD])
            deadline = st.sites[BAD]["quarantined_until"]
            for poll in range(1, 8):
                self.assertEqual(st.blacklist(now=100.0 + poll * 900.0), [BAD])
            self.assertEqual(st.sites[BAD]["quarantines"], 1)
            self.assertEqual(st.sites[BAD]["quarantined_until"], deadline)

    def test_a_record_from_before_the_escalation_still_loads(self):
        """Its site simply starts at the base duration, which is what it had anyway."""
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "crab_site_stats.json")
            with open(path, "w") as f:
                json.dump(
                    {
                        "version": 1,
                        "sites": {
                            BAD: {"events": [[0.0, 0]], "quarantined_until": 0.0}
                        },
                    },
                    f,
                )
            st = SiteStats(path)
            self.assertEqual(st.sites[BAD]["quarantines"], 0)
            self.assertEqual(st.sites[BAD]["cleared_at"], 0.0)
            self.assertEqual(len(st.sites[BAD]["events"]), 1)


class TheRecordSurvivesTheQuarantine(unittest.TestCase):
    """`_expire` used to empty `events`, which is what let a broken site start over."""

    def test_expiry_itself_no_longer_empties_the_record(self):
        """A short ban, so that the rolling window cannot be what removes the evidence."""
        with tempfile.TemporaryDirectory() as d:
            st = stats(d, quarantine_hours=1.0)
            make_it_fail(st, 0.0)
            st.blacklist(now=100.0)
            n_before = len(st.sites[BAD]["events"])
            st.blacklist(now=st.sites[BAD]["quarantined_until"] + 1.0)
            self.assertEqual(
                len(st.sites[BAD]["events"]),
                n_before,
                "the failures that earned the ban were forgotten",
            )

    def test_over_a_long_ban_it_is_the_count_that_carries_the_history(self):
        """With the defaults the ban (24 h) is as long as the window (24 h), so the outcomes age
        out of the rate measurement by themselves -- as they should, a year-old failure says
        nothing about a site today. The escalation count is what has to survive, and does.
        """
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            make_it_fail(st, 0.0)
            st.blacklist(now=100.0)
            st.blacklist(now=st.sites[BAD]["quarantined_until"] + 1.0)
            self.assertEqual(st.sites[BAD]["events"], [])
            self.assertEqual(st.sites[BAD]["quarantines"], 1)

    def test_the_count_survives_a_save_and_reload(self):
        """It is what the next ban's length is computed from, so it has to be persisted."""
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            make_it_fail(st, 0.0)
            st.blacklist(now=100.0)
            st.save()
            with open(st.path) as f:
                self.assertEqual(json.load(f)["sites"][BAD]["quarantines"], 1)
            again = stats(d)
            self.assertEqual(again.sites[BAD]["quarantines"], 1)

    def test_the_moment_a_ban_ended_survives_a_restart_too(self):
        """A driver restarted between the expiry and the next wave would otherwise re-quarantine
        the site instantly, on the pre-ban evidence -- `cleared_at` is as load-bearing as the
        count, and is only ever written by `_expire`."""
        with tempfile.TemporaryDirectory() as d:
            st = stats(d, quarantine_hours=1.0)
            make_it_fail(st, 0.0)
            st.blacklist(now=100.0)
            lifted = st.sites[BAD]["quarantined_until"] + 1.0
            st.blacklist(now=lifted)
            st.save()
            again = stats(d)
            self.assertEqual(
                again.sites[BAD]["cleared_at"], st.sites[BAD]["cleared_at"]
            )
            self.assertEqual(
                again.blacklist(now=lifted + 1.0),
                [],
                "a restart re-quarantined the site on the evidence its ban was served for",
            )


class ASecondChanceIsReallyGiven(unittest.TestCase):
    """The hazard that keeping the record creates, and the reason `cleared_at` exists."""

    def test_a_lifted_ban_does_not_re_arm_on_the_old_failures(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            make_it_fail(st, 0.0)
            st.blacklist(now=100.0)
            lifted = st.sites[BAD]["quarantined_until"] + 1.0
            self.assertEqual(
                st.blacklist(now=lifted),
                [],
                "re-quarantined on the very evidence the ban was served for",
            )
            # and it stays out of the blacklist while it does nothing wrong
            self.assertEqual(st.blacklist(now=lifted + HOUR), [])

    def test_but_one_fresh_generation_of_failures_is_enough(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            make_it_fail(st, 0.0)
            st.blacklist(now=100.0)
            lifted = st.sites[BAD]["quarantined_until"] + 1.0
            make_it_fail(st, lifted)
            self.assertEqual(st.blacklist(now=lifted + 100.0), [BAD])
            self.assertAlmostEqual(
                quarantine_span(st, BAD, lifted + 100.0), 48.0, places=3
            )

    def test_a_site_that_comes_back_healthy_is_never_re_quarantined(self):
        with tempfile.TemporaryDirectory() as d:
            st = stats(d)
            make_it_fail(st, 0.0)
            st.blacklist(now=100.0)
            lifted = st.sites[BAD]["quarantined_until"] + 1.0
            for i in range(30):
                st.record(BAD, True, now=lifted + i)
            for i in range(40):
                st.record(GOOD, True, now=lifted + i)
            self.assertEqual(st.blacklist(now=lifted + 100.0), [])


if __name__ == "__main__":
    unittest.main()
