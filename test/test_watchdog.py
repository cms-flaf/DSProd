#!/usr/bin/env python3
"""When a job that still holds its slot is declared dead, and -- mostly -- when it is not.

The failure this exists for: two jobs of a 600-branch production reported `running` with a status
record byte-identical across eight polls, having stopped reporting 16 h earlier, and nothing would
have reclaimed them for another 8 h. The evidence is a flag file per job whose modification time
the job itself advances.

Most of what follows tests the cases where a verdict must NOT be issued, because every one of them
costs a branch one of its four attempts and ~30 exhausted branches of a 600-job run end the whole
workflow. The dangerous failure is not a missed stall; it is a watchdog that condemns healthy jobs.
"""

import datetime
import os
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod.watchdog import DEFAULTS, StallWatchdog, watchdog_config  # noqa: E402

NOW = datetime.datetime(2026, 9, 8, 12, 0, 0)
TASK = "260908_000000:kandroso_crab_RunProd_x"


class Flag:
    """What one entry of `gfal_ls` gives the driver: a name, and a modification time."""

    def __init__(self, name, age_minutes):
        self.name = str(name)
        self.date = NOW - datetime.timedelta(minutes=age_minutes)
        self.is_dir = False


def watchdog(flags, cfg=None, listing_fails=False):
    # `cfg=False` means "switched off", which is not the same as "no settings given"
    raw = {} if cfg is None else cfg
    w = StallWatchdog("root://x//flags", watchdog_config({"watchdog": raw}))
    w.publish = lambda msg: w.messages.append(msg)
    w.messages = []
    with mock.patch(
        "dsprod.watchdog.gfal_ls_safe",
        return_value=None if listing_fails else list(flags),
    ):
        w.refresh()
    return w


def jobs(*specs):
    """law's job_data: job_num -> {job_id, branches}."""
    return {
        str(num): {"job_id": [num, TASK, "/proj"], "branches": [branch]}
        for num, branch in specs
    }


def status(*nums, state="running"):
    return {
        (num, TASK, "/proj"): {"status": state, "extra": {"site_history": ["T2_X"]}}
        for num in nums
    }


def prime(w, job_map, first_seen_minutes_ago=999):
    """Publish the job map and backdate the grace clock, without forming a verdict."""
    w.set_jobs(job_map)
    for entry in job_map.values():
        w._first_running[w._key(entry["job_id"])] = NOW - datetime.timedelta(
            minutes=first_seen_minutes_ago
        )


def verdicts(w, job_map, result, first_seen_minutes_ago=999):
    w.set_jobs(job_map)
    # pretend these jobs have been polled as running for a while, so the grace period is past
    for entry in job_map.values():
        w._first_running[w._key(entry["job_id"])] = NOW - datetime.timedelta(
            minutes=first_seen_minutes_ago
        )
    return w.verdicts(result, now=NOW)


class AVerdictIsIssued(unittest.TestCase):
    def test_when_the_flag_has_not_moved_for_the_configured_span(self):
        w = watchdog([Flag(7, age_minutes=61)])
        out = verdicts(w, jobs((1, 7)), status(1))
        self.assertEqual(len(out), 1)
        self.assertIn("61 min old", list(out.values())[0])

    def test_when_a_job_never_wrote_a_flag_at_all_and_the_grace_is_past(self):
        w = watchdog([])
        self.assertEqual(len(verdicts(w, jobs((1, 7)), status(1))), 1)

    def test_the_threshold_is_interval_times_missed_checks(self):
        cfg = {"interval_minutes": 10, "missed_checks": 3}
        self.assertEqual(
            len(verdicts(watchdog([Flag(7, 31)], cfg), jobs((1, 7)), status(1))), 1
        )
        self.assertEqual(
            len(verdicts(watchdog([Flag(7, 29)], cfg), jobs((1, 7)), status(1))), 0
        )


class NoVerdictIsIssued(unittest.TestCase):
    def test_when_the_flag_is_fresh(self):
        w = watchdog([Flag(7, age_minutes=5)])
        self.assertEqual(verdicts(w, jobs((1, 7)), status(1)), {})

    def test_when_the_job_is_not_running(self):
        """Condemning a branch that has already finished is the worst false positive here."""
        w = watchdog([Flag(7, age_minutes=999)])
        for state in ("finished", "pending", "failed"):
            self.assertEqual(verdicts(w, jobs((1, 7)), status(1, state=state)), {})

    def test_when_the_job_has_not_had_time_to_write_its_first_flag(self):
        w = watchdog([])
        self.assertEqual(
            verdicts(w, jobs((1, 7)), status(1), first_seen_minutes_ago=5), {}
        )

    def test_when_the_listing_could_not_be_read(self):
        """No listing is no evidence -- and an outage must not accumulate staleness either."""
        w = watchdog([], listing_fails=True)
        self.assertEqual(verdicts(w, jobs((1, 7)), status(1)), {})

    def test_when_most_running_jobs_look_stale(self):
        """Writing to the storage can break while reading it still works, and then every flag
        goes stale at once while every job is perfectly healthy."""
        w = watchdog([Flag(b, age_minutes=99) for b in range(10)])
        out = verdicts(w, jobs(*[(n, n) for n in range(10)]), status(*range(10)))
        self.assertEqual(out, {})
        self.assertTrue(any("storage fault" in m for m in w.messages), w.messages)

    def test_beyond_the_per_interval_cap(self):
        w = watchdog(
            [Flag(b, age_minutes=99) for b in range(3)],
            cfg={"max_per_interval": 2, "max_stale_fraction": 1.0},
        )
        out = verdicts(w, jobs(*[(n, n) for n in range(3)]), status(*range(3)))
        self.assertEqual(len(out), 2)
        self.assertTrue(any("max_per_interval" in m for m in w.messages), w.messages)

    def test_for_a_branch_that_has_already_been_rescued_once(self):
        """A branch that stalls wherever it runs is the branch's problem, not the slot's, and
        each verdict spends one of its four attempts."""
        w = watchdog([Flag(7, age_minutes=99)])
        self.assertEqual(len(verdicts(w, jobs((1, 7)), status(1))), 1)
        again = verdicts(w, jobs((2, 7)), status(2))
        self.assertEqual(again, {})
        self.assertTrue(any("stalled 2 times" in m for m in w.messages), w.messages)

    def test_in_dry_run(self):
        w = watchdog([Flag(7, age_minutes=99)], cfg={"dry_run": True})
        self.assertEqual(verdicts(w, jobs((1, 7)), status(1)), {})
        self.assertTrue(any("dry run" in m for m in w.messages), w.messages)

    def test_when_the_watchdog_is_switched_off(self):
        w = watchdog([Flag(7, age_minutes=99)], cfg=False)
        self.assertFalse(w.enabled)
        self.assertEqual(verdicts(w, jobs((1, 7)), status(1)), {})

    def test_when_a_flag_timestamp_is_in_the_future(self):
        """Driver-to-storage clock skew must read as fresh, not as a huge negative age."""
        w = watchdog([Flag(7, age_minutes=-30)])
        self.assertEqual(verdicts(w, jobs((1, 7)), status(1)), {})


class TheSettings(unittest.TestCase):
    def test_it_is_on_by_default(self):
        self.assertTrue(watchdog_config({})["enabled"])
        self.assertEqual(watchdog_config({})["interval_minutes"], 30)
        self.assertEqual(watchdog_config({})["missed_checks"], 2)

    def test_false_switches_it_off_wholesale(self):
        self.assertFalse(watchdog_config({"watchdog": False})["enabled"])

    def test_a_misspelled_setting_is_refused_rather_than_ignored(self):
        with self.assertRaises(RuntimeError) as caught:
            watchdog_config({"watchdog": {"intervall_minutes": 5}})
        self.assertIn("intervall_minutes", str(caught.exception))

    def test_nonsense_intervals_are_refused(self):
        for bad in ({"interval_minutes": 0}, {"missed_checks": 0}):
            with self.assertRaises(RuntimeError):
                watchdog_config({"watchdog": bad})

    def test_every_default_is_documented_in_the_config_reference(self):
        with open(os.path.join(dsprod_repo, "config", "global.yaml")) as f:
            text = f.read()
        for key in DEFAULTS:
            self.assertIn(key, text, f"{key} is not mentioned in config/global.yaml")


class HowTheVerdictReachesLaw(unittest.TestCase):
    """The verdict is applied by rewriting the status law just fetched, so law's own retry path
    does the resubmission -- there is deliberately no second mechanism."""

    def manager(self, watchdog_obj):
        from dsprod.crab import DSProdCrabJobManager

        mgr = mock.Mock()
        mgr.RUNNING, mgr.FAILED = "running", "failed"
        mgr.watchdog = watchdog_obj
        mgr._stats_lock = __import__("threading").Lock()
        mgr._stats_seen = set()
        mgr.site_stats = mock.Mock()
        mgr._apply_watchdog = DSProdCrabJobManager._apply_watchdog.__get__(mgr)
        return mgr

    def test_a_stalled_job_is_rewritten_as_failed(self):
        w = watchdog([Flag(7, age_minutes=99)])
        result = status(1)
        prime(w, jobs((1, 7)))
        with mock.patch("dsprod.watchdog.datetime") as dt:
            dt.datetime.utcnow.return_value = NOW
            self.manager(w)._apply_watchdog(result)
        data = list(result.values())[0]
        self.assertEqual(data["status"], "failed")
        self.assertIn("stalled", data["error"])

    def test_the_failure_carries_no_code_so_it_stays_out_of_the_site_record(self):
        """`harvest_site_stats` skips a failure with no job-level code; the site is recorded once,
        explicitly, instead of every stall of the same branch hitting a different site.
        """
        w = watchdog([Flag(7, age_minutes=99)])
        result = status(1)
        prime(w, jobs((1, 7)))
        mgr = self.manager(w)
        with mock.patch("dsprod.watchdog.datetime") as dt:
            dt.datetime.utcnow.return_value = NOW
            mgr._apply_watchdog(result)
        self.assertIsNone(list(result.values())[0]["code"])
        mgr.site_stats.record.assert_called_once_with("T2_X", False)

    def test_a_job_that_finished_in_the_meantime_is_left_alone(self):
        w = watchdog([Flag(7, age_minutes=99)])
        result = status(1)
        prime(w, jobs((1, 7)))
        mgr = self.manager(w)
        # it completes in the seconds between the verdict being formed and applied
        original_verdicts = w.verdicts

        def finish_then_report(res, now=None):
            out = original_verdicts(res, now=now)
            list(res.values())[0]["status"] = "finished"
            return out

        w.verdicts = finish_then_report
        with mock.patch("dsprod.watchdog.datetime") as dt:
            dt.datetime.utcnow.return_value = NOW
            mgr._apply_watchdog(result)
        self.assertEqual(list(result.values())[0]["status"], "finished")
        mgr.site_stats.record.assert_not_called()

    def test_nothing_happens_when_the_watchdog_is_off(self):
        w = watchdog([Flag(7, age_minutes=99)], cfg=False)
        result = status(1)
        mgr = self.manager(w)
        mgr._apply_watchdog(result)
        self.assertEqual(list(result.values())[0]["status"], "running")


if __name__ == "__main__":
    unittest.main()
