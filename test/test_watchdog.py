#!/usr/bin/env python3
"""When a job that still holds its slot is declared dead, and -- mostly -- when it is not.

The failure this exists for: two jobs of a 600-branch production reported `running` with a status
record byte-identical across eight polls, having stopped reporting 16 h earlier, and nothing would
have reclaimed them for another 8 h. The evidence is a flag file per job whose modification time
the job itself advances.

Most of what follows tests the cases where a verdict must NOT be issued, because every one of them
costs a branch one of its attempts and ~30 exhausted branches of a 600-job run end the whole
workflow. The dangerous failure is not a missed stall; it is a watchdog that condemns healthy jobs.
"""

import datetime
import math
import os
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod import grid_tools, tools  # noqa: E402
from dsprod.watchdog import (  # noqa: E402
    DEFAULTS,
    MIN_INTERVAL_MINUTES,
    Heartbeat,
    StallWatchdog,
    watchdog_config,
)

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
    """Publish the job map and backdate when the jobs were first seen running, on both
    clocks (the listing's, for the grace, and the query's, for which flags count), without
    forming a verdict."""
    w.set_jobs(job_map)
    for entry in job_map.values():
        key = w._key(entry["job_id"])
        first_seen = NOW - datetime.timedelta(minutes=first_seen_minutes_ago)
        w._first_running[key] = first_seen
        w._first_seen[key] = first_seen


def verdicts(w, job_map, result, first_seen_minutes_ago=999):
    # pretend these jobs have been polled as running for a while, so the grace period is past
    prime(w, job_map, first_seen_minutes_ago)
    return w.verdicts(result, now=NOW)


def relist(w, flags):
    with mock.patch("dsprod.watchdog.gfal_ls_safe", return_value=flags):
        return w.refresh()


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

    def test_an_unreadable_directory_is_reported_once_not_every_interval(self):
        """It does not exist until the first job writes a flag, so the raw CLI error would be
        printed on every interval of every wave and bury the case worth noticing."""
        w = watchdog([], listing_fails=True)
        with mock.patch("dsprod.watchdog.gfal_ls_safe", return_value=None):
            w.refresh()
            w.refresh()
        self.assertEqual(
            len([m for m in w.messages if "cannot list" in m]), 1, w.messages
        )

    def test_becoming_readable_again_is_reported(self):
        w = watchdog([], listing_fails=True)
        with mock.patch("dsprod.watchdog.gfal_ls_safe", return_value=[Flag(7, 1)]):
            w.refresh()
        self.assertTrue(any("readable again" in m for m in w.messages), w.messages)

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
        each verdict spends one of its attempts."""
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


class TheStorageFaultGuardNeedsEnoughRunningJobs(unittest.TestCase):
    """The incident this exists for was the tail of a production: 598 of 600 branches done,
    the last two stalled. Read as a proportion, two stale flags out of two running jobs is
    100 %, which the storage-fault guard took for a broken storage, so the very jobs the
    watchdog is for were never rescued. The proportion is read only from
    `min_running_for_fault_guard` running jobs up; below that, stale jobs are judged one by
    one, still bounded by `max_per_interval` and `max_per_branch`."""

    def stale_of_running(self, n_running, n_stale):
        """`n_running` jobs running, one branch each, of which the first `n_stale` stalled."""
        flags = [
            Flag(b, age_minutes=99 if b < n_stale else 5) for b in range(n_running)
        ]
        w = watchdog(flags)
        job_map = jobs(*[(n, n) for n in range(n_running)])
        return w, verdicts(w, job_map, status(*range(n_running)))

    def assertNoStorageFault(self, w):
        self.assertFalse(any("storage fault" in m for m in w.messages), w.messages)

    def test_the_last_two_running_jobs_both_stalled_are_both_failed(self):
        w, out = self.stale_of_running(2, 2)
        self.assertEqual(len(out), 2)
        self.assertNoStorageFault(w)

    def test_nine_of_nine_stale_are_failed_up_to_the_per_interval_cap(self):
        w, out = self.stale_of_running(9, 9)
        self.assertEqual(len(out), DEFAULTS["max_per_interval"])
        self.assertNoStorageFault(w)
        self.assertTrue(any("max_per_interval" in m for m in w.messages), w.messages)

    def test_ten_running_with_six_stale_is_read_as_a_storage_fault(self):
        w, out = self.stale_of_running(10, 6)
        self.assertEqual(out, {})
        self.assertTrue(any("storage fault" in m for m in w.messages), w.messages)

    def test_ten_running_with_five_stale_get_their_verdicts(self):
        w, out = self.stale_of_running(10, 5)
        self.assertEqual(len(out), 5)
        self.assertNoStorageFault(w)

    def test_a_write_fault_in_the_tail_costs_each_branch_one_attempt_at_most(self):
        """The price of reading a few jobs at face value: a storage that stops taking
        writes while only a few jobs run gets them all failed -- once. Their resubmissions
        stall the same way and are left to CRAB's wall-clock limit."""
        flags = [Flag(b, age_minutes=99) for b in range(3)]
        w = watchdog(flags)
        self.assertEqual(
            len(verdicts(w, jobs((0, 0), (1, 1), (2, 2)), status(0, 1, 2))), 3
        )
        relist(w, flags)  # the next interval: the per-interval cap starts again
        resubmitted = jobs((10, 0), (11, 1), (12, 2))
        self.assertEqual(verdicts(w, resubmitted, status(10, 11, 12)), {})
        self.assertNoStorageFault(w)


class AFlagThatDisappears(unittest.TestCase):
    """Found by the first dry-run wave, which issued two `no heartbeat` verdicts against a job
    that had just finished successfully: the heartbeat context removes the flag on the way out,
    and CRAB keeps reporting the job as running for minutes afterwards. Armed, that would have
    resubmitted a branch whose product had just been written."""

    def setUp(self):
        self.w = watchdog([Flag(7, age_minutes=0)])
        prime(self.w, jobs((1, 7)))
        # the driver has seen the flag at least once
        self.assertEqual(self.w.verdicts(status(1), now=NOW), {})

    def _relist(self, flags):
        with mock.patch("dsprod.watchdog.gfal_ls_safe", return_value=flags):
            self.w.refresh()

    def test_a_job_that_had_a_flag_and_lost_it_is_not_condemned(self):
        self._relist([])
        self.assertEqual(self.w.verdicts(status(1), now=NOW), {})
        self.assertTrue(
            any("exiting or restarting" in m for m in self.w.messages), self.w.messages
        )

    def test_a_job_that_never_had_one_still_is(self):
        """The other half: nothing to distinguish it from a job that never started its payload."""
        w = watchdog([])
        self.assertEqual(len(verdicts(w, jobs((2, 9)), status(2))), 1)

    def test_a_worker_that_dies_without_exiting_leaves_its_flag_and_is_caught(self):
        """The shape of the real incident: the process stops, the flag stays and goes stale."""
        self._relist([Flag(7, age_minutes=99)])
        out = self.w.verdicts(status(1), now=NOW)
        self.assertEqual(len(out), 1)
        self.assertIn("99 min old", list(out.values())[0])


class WhatGetsSaidOnceOnly(unittest.TestCase):
    """The poll loop revisits the same jobs every interval, so anything published from inside it
    repeats until the job leaves. The first wave printed the same dry-run verdict 14 times.
    """

    def test_a_dry_run_verdict_is_published_once_per_job(self):
        w = watchdog([Flag(7, age_minutes=99)], cfg={"dry_run": True})
        prime(w, jobs((1, 7)))
        for _ in range(5):
            w.verdicts(status(1), now=NOW)
        self.assertEqual(len([m for m in w.messages if "dry run" in m]), 1, w.messages)

    def test_each_job_gets_its_own_dry_run_verdict(self):
        """The note is keyed by the job it is about, not by whichever job the status loop saw
        last, or two stalled jobs in one poll dedupe against each other."""
        w = watchdog(
            [Flag(7, age_minutes=99), Flag(8, age_minutes=99)],
            cfg={"dry_run": True, "max_stale_fraction": 1.0},
        )
        self.assertEqual(verdicts(w, jobs((1, 7), (2, 8)), status(1, 2)), {})
        dry = [m for m in w.messages if "dry run" in m]
        self.assertEqual(len(dry), 2, w.messages)
        self.assertTrue(any("job 1 " in m for m in dry), dry)
        self.assertTrue(any("job 2 " in m for m in dry), dry)

    def test_the_per_branch_cap_is_explained_once(self):
        w = watchdog([Flag(7, age_minutes=99)])
        prime(w, jobs((1, 7)))
        w.verdicts(status(1), now=NOW)  # spends the branch's one rescue
        for _ in range(4):
            w.verdicts(status(1), now=NOW)
        self.assertEqual(
            len([m for m in w.messages if "stalled 2 times" in m]), 1, w.messages
        )


class AJobLawHasReplaced(unittest.TestCase):
    """`forget()` is called for a job id the watchdog has just failed, so that if the id is
    seen running again it starts clean, on both clocks."""

    def test_the_grace_clock_starts_again(self):
        w = watchdog([])
        prime(w, jobs((1, 7)))
        w.forget((1, TASK, "/proj"))
        # first seen running now, so the missing flag is not evidence yet
        self.assertEqual(w.verdicts(status(1), now=NOW), {})

    def test_the_flag_it_left_is_not_evidence_against_what_runs_next(self):
        # a second rescue allowed, so that only the flag decides
        w = watchdog([Flag(7, age_minutes=99)], cfg={"max_per_branch": 2})
        self.assertEqual(len(verdicts(w, jobs((1, 7)), status(1))), 1)
        w.forget((1, TASK, "/other_proj"))
        relist(w, [Flag(7, age_minutes=99)])
        self.assertEqual(w.verdicts(status(1), now=NOW), {})


def at(n):
    """`n` minutes after NOW."""
    return NOW + datetime.timedelta(minutes=n)


class AStaleFlagLeftByAnEarlierAttempt(unittest.TestCase):
    """Flags are named by branch, so a worker that died without removing its flag leaves one
    behind under the name the next attempt of the same branch will use. Read as evidence about
    the attempt running now, it condemns that attempt on the first poll that sees it running,
    before its own first beat -- and spends one of the branch's attempts for nothing.

    Defaults throughout: a beat every 30 min, stale after 2 missed (60 min), and a job that
    never beats is given 90 min. The verdicts here are given the clock of the query;
    `AnEarlierAttemptsFlagOnTheListingClock` drives the same case as production does.
    """

    def setUp(self):
        self.w = StallWatchdog("root://x//flags", watchdog_config({}))
        self.w.publish = lambda msg: None
        # the new attempt: job 1 of a fresh CRAB task, running branch 7
        self.w.set_jobs({1: {"job_id": [1, TASK, "/proj"], "branches": [7]}})
        self.key = (1, TASK, "/proj")

    def poll(self, n):
        return self.w.verdicts({self.key: {"status": "running"}}, now=at(n))

    def test_it_does_not_condemn_the_new_attempt_on_its_first_running_poll(self):
        self.assertTrue(relist(self.w, [Flag(7, age_minutes=120)]))
        self.assertEqual(self.poll(0), {})

    def test_nor_on_any_later_poll_inside_the_grace_for_a_first_beat(self):
        relist(self.w, [Flag(7, age_minutes=120)])
        for n in (0, 30, 60, 89):
            self.assertEqual(self.poll(n), {}, f"condemned {n} min after first seen")

    def test_a_new_attempt_that_never_beats_is_still_condemned_after_the_grace(self):
        relist(self.w, [Flag(7, age_minutes=120)])
        self.assertEqual(self.poll(0), {})
        verdict = self.poll(90)
        self.assertEqual(list(verdict), [self.key])
        self.assertIn("no heartbeat", verdict[self.key])

    def test_the_new_attempts_own_flag_going_stale_still_condemns_it(self):
        relist(self.w, [Flag(7, age_minutes=120)])
        self.assertEqual(self.poll(0), {})
        # its first beat overwrites the old flag in place
        relist(self.w, [Flag(7, age_minutes=-5)])
        self.assertEqual(self.poll(10), {})
        self.assertEqual(self.poll(64), {})
        verdict = self.poll(65)
        self.assertEqual(list(verdict), [self.key])
        self.assertIn("heartbeat 60 min old", verdict[self.key])

    def test_a_beat_listed_just_before_the_job_was_first_seen_running_counts(self):
        # listed modification times have minute resolution, so the first beat of a job that
        # started within the minute before the poll lists as up to a minute early
        relist(self.w, [Flag(7, age_minutes=1)])
        self.assertEqual(self.poll(0), {})
        verdict = self.poll(61)
        self.assertEqual(list(verdict), [self.key])
        self.assertIn("heartbeat 62 min old", verdict[self.key])

    def test_the_same_poll_condemns_it_when_attempts_are_not_told_apart(self):
        # judging every flag of the branch, whoever wrote it: the verdict that telling the
        # attempts apart exists to withhold
        original = StallWatchdog._age

        def age_without_since(self, branches, now, since=None):
            return original(self, branches, now)

        relist(self.w, [Flag(7, age_minutes=120)])
        with mock.patch.object(StallWatchdog, "_age", age_without_since):
            verdict = self.poll(0)
        self.assertEqual(list(verdict), [self.key])
        self.assertIn("heartbeat 120 min old", verdict[self.key])


class TheListing(unittest.TestCase):
    def test_it_is_quiet_names_the_proxy_and_is_bounded(self):
        w = StallWatchdog("root://x//flags", watchdog_config({}), voms_token="/tmp/x")
        with mock.patch("dsprod.watchdog.gfal_ls_safe", return_value=[]) as ls:
            self.assertTrue(w.refresh())
        ls.assert_called_once_with(
            "root://x//flags",
            voms_token="/tmp/x",
            catch_stderr=True,
            verbose=0,
            timeout=300,
        )

    def test_a_hanging_listing_cannot_hold_the_poll_loop(self):
        """The listing runs inside law's poll loop, and gfal's own limit is half an hour."""
        self.assertEqual(StallWatchdog.listing_timeout_seconds, 300)

    @staticmethod
    def command(fn, *args, **kwargs):
        with mock.patch.object(
            grid_tools, "ps_call", return_value=(0, [], None)
        ) as call:
            fn(*args, **kwargs)
        (cmd,), _ = call.call_args
        return cmd

    def test_a_best_effort_listing_passes_its_bound_to_gfal(self):
        cmd = self.command(
            grid_tools.gfal_ls_safe, "davs://host/x", voms_token="t", timeout=300
        )
        self.assertEqual(cmd[0], "gfal-ls")
        self.assertEqual(cmd[cmd.index("--timeout") + 1], "300")
        self.assertEqual(cmd[-1], "davs://host/x")

    def test_a_listing_without_a_bound_keeps_the_gfal_default(self):
        for fn in (grid_tools.gfal_ls_safe, grid_tools.gfal_ls):
            with self.subTest(fn=fn.__name__):
                cmd = self.command(fn, "davs://host/x", voms_token="t")
                self.assertNotIn("--timeout", cmd)
                self.assertNotIn("-t", cmd)

    @unittest.skipUnless(shutil.which("gfal-ls"), "gfal-ls is not installed")
    def test_the_real_gfal_ls_accepts_the_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            open(os.path.join(tmp, "7"), "w").close()
            entries = grid_tools.gfal_ls_safe(
                "file://" + tmp,
                voms_token="/none",
                catch_stderr=True,
                verbose=0,
                timeout=30,
            )
        self.assertIsNotNone(entries, "gfal-ls refused the command line")
        self.assertEqual([e.name for e in entries], ["7"])


class Clock:
    """The `datetime` module as the watchdog and the poll callback's throttle
    (`timed_call_wrapper`) see it: one settable time, for the driver and the storage alike.
    """

    timedelta = datetime.timedelta

    def __init__(self, start):
        self.time = start
        self.datetime = types.SimpleNamespace(utcnow=self.read, now=self.read)

    def read(self):
        return self.time


class AgesAreMeasuredAtTheListing(unittest.TestCase):
    """`refresh()` runs once per interval and `verdicts()` on every poll in between, so every
    age is measured at the time the listing behind it was taken, never at the time of the
    poll that reads it (`OverManyIntervals` below shows what the poll's clock did)."""

    def listed_at(self, when, flags):
        w = StallWatchdog("root://x//flags", watchdog_config({}))
        w.publish = lambda msg: None
        with mock.patch("dsprod.watchdog.gfal_ls_safe", return_value=list(flags)):
            self.assertTrue(w.refresh(now=when))
        return w

    def test_a_time_given_to_refresh_is_the_time_ages_are_measured_at(self):
        w = self.listed_at(NOW, [Flag(7, age_minutes=30), Flag(8, age_minutes=61)])
        prime(w, jobs((1, 7), (2, 8)))
        # a day later on the driver's clock, by which both flags would be stale
        with mock.patch("dsprod.watchdog.datetime", Clock(at(24 * 60))):
            out = w.verdicts(status(1, 2))
        self.assertEqual([job_id[0] for job_id in out], [2])
        self.assertIn("61 min old", list(out.values())[0])

    def test_the_time_is_taken_before_the_listing(self):
        """A flag beaten while the listing ran is no older than the moment it started, and a
        listing may take up to its timeout."""
        clock = Clock(NOW)
        w = StallWatchdog("root://x//flags", watchdog_config({}))

        def slow_listing(*args, **kwargs):
            clock.time = at(10)
            return [Flag(7, age_minutes=55)]

        with mock.patch("dsprod.watchdog.datetime", clock), mock.patch(
            "dsprod.watchdog.gfal_ls_safe", side_effect=slow_listing
        ):
            self.assertTrue(w.refresh())
            prime(w, jobs((1, 7)))
            self.assertEqual(w.verdicts(status(1)), {})
        self.assertEqual(w._listed_at, NOW)

    def test_a_failed_listing_forgets_when_the_last_good_one_was_taken(self):
        w = self.listed_at(NOW, [Flag(7, age_minutes=5)])
        self.assertEqual(w._listed_at, NOW)
        with mock.patch("dsprod.watchdog.gfal_ls_safe", return_value=None):
            self.assertFalse(w.refresh(now=at(30)))
        self.assertIsNone(w._listed_at)
        self.assertEqual(verdicts(w, jobs((1, 7)), status(1)), {})


# --------------------------------------------------------------------------------------
# a driver over many intervals: verdicts on every poll, one listing per interval
# --------------------------------------------------------------------------------------

#: seconds between two polls of the driver
POLL = 300
#: seconds a beat takes to land; Heartbeat._loop waits an interval after each one, so beats
#: drift against the driver's grid by this much per interval
WRITE = 5
INTERVAL = DEFAULTS["interval_minutes"] * 60
STALE = INTERVAL * DEFAULTS["missed_checks"]
T0 = datetime.datetime(2026, 9, 8, 0, 0, 0)


class SimJob:
    """One CRAB job: when CRAB reports it running, and when its beats land on the storage."""

    def __init__(
        self,
        num,
        branch,
        running_from,
        beats_from,
        beats_until=None,
        running_until=None,
    ):
        self.num = num
        self.branch = branch
        self.running_from = running_from
        self.running_until = running_until
        self.beats_from = beats_from
        self.beats_until = beats_until

    def running(self, t):
        return self.running_from <= t and (
            self.running_until is None or t < self.running_until
        )

    def last_beat(self, t):
        """When the newest beat landed by `t`, or None before the first one."""
        if self.beats_until is not None:
            t = min(t, self.beats_until)
        n = math.floor((t - self.beats_from - WRITE) / (INTERVAL + WRITE))
        if n < 0:
            return None
        return self.beats_from + WRITE + n * (INTERVAL + WRITE)


def stamp(seconds):
    """A modification time as `gfal-ls --time-style long-iso` lists it: to the minute."""
    return (T0 + datetime.timedelta(seconds=seconds)).replace(second=0, microsecond=0)


def simulate(sim_jobs, hours, query_clock=False):
    """Drive a real watchdog for `hours` the way the CRAB backend does.

    Every POLL seconds the job manager's query forms the verdicts on the jobs CRAB reports
    running -- without a time of its own, as `_apply_watchdog` asks for them -- and then the
    poll callback lists the flag directory through the throttle `crab_poll_callback` uses.
    `query_clock` measures the verdicts against the clock of the query instead, as they were
    before ages were measured at the listing. A condemned job is failed by law and no longer
    reported running.
    """
    clock = Clock(T0)
    w = StallWatchdog("root://x//flags", watchdog_config({}))
    w.messages = []
    w.publish = w.messages.append
    w.set_jobs(jobs(*[(job.num, job.branch) for job in sim_jobs]))
    failed = {}
    listings = []

    def listing(*args, **kwargs):
        t = (clock.time - T0).total_seconds()
        listings.append(t)
        newest = {}
        for job in sim_jobs:
            beat = job.last_beat(t)
            if beat is not None:
                # flags are named by branch: a later attempt's beat replaces the flag
                newest[job.branch] = max(beat, newest.get(job.branch, beat))
        return [
            types.SimpleNamespace(name=str(branch), date=stamp(beat), is_dir=False)
            for branch, beat in newest.items()
        ]

    with mock.patch("dsprod.watchdog.datetime", clock), mock.patch.object(
        tools, "datetime", clock
    ), mock.patch("dsprod.watchdog.gfal_ls_safe", side_effect=listing):
        refresh = tools.timed_call_wrapper(w.refresh, w.interval_seconds)
        for t in range(0, int(hours * 3600) + 1, POLL):
            clock.time = T0 + datetime.timedelta(seconds=t)
            nums = [j.num for j in sim_jobs if j.running(t) and j.num not in failed]
            out = w.verdicts(status(*nums), now=clock.time if query_clock else None)
            for job_id, reason in out.items():
                failed[job_id[0]] = (t, reason)
            refresh()
    return types.SimpleNamespace(failed=failed, listings=listings, watchdog=w)


class OverManyIntervals(unittest.TestCase):
    """The listing is refreshed once per interval, after the query of the poll that
    refreshes it, while verdicts are formed on every poll. Measured against the clock of the
    query, a flag beaten just after one listing is shown by the next at up to an interval old
    and read until the one after at up to two, so the last poll before a listing failed a
    healthy job ("heartbeat 60 min old", threshold 60 min). Measured at the listing, a healthy
    flag is never older than one beat period plus the minute the listing rounds down to.

    Default settings, each case for 21 hours. The phases sweep the beats across a whole
    listing period, and so across the poll grid too: the 5 s a beat drifts per interval
    covers only 210 s of it in 21 hours.
    """

    PHASES = range(0, INTERVAL, 7)
    HOURS = 21

    def healthy(self, phase):
        return SimJob(1, 7, running_from=600 + phase, beats_from=720 + phase)

    def test_a_healthy_job_is_never_failed(self):
        for phase in self.PHASES:
            with self.subTest(phase=phase):
                run = simulate([self.healthy(phase)], self.HOURS)
                self.assertEqual(run.failed, {})
                self.assertEqual(run.watchdog.messages, [])
                # not vacuous: its flag was listed and judged throughout
                self.assertGreaterEqual(len(run.listings), 2 * self.HOURS)
                self.assertIn((1, TASK), run.watchdog._seen_flag)

    def test_measured_at_the_query_the_same_healthy_job_is_failed(self):
        """The test above, run against the clock the verdicts used before the fix."""
        condemned = {}
        for phase in self.PHASES:
            run = simulate([self.healthy(phase)], self.HOURS, query_clock=True)
            if run.failed:
                condemned[phase] = run.failed[1]
        self.assertTrue(condemned, "the query clock never failed the healthy job")
        for t, reason in condemned.values():
            self.assertIn("min old", reason)
            # always the last poll before a listing, whose query precedes the refresh
            self.assertEqual(t % INTERVAL, 0, (t, reason))

    def test_a_job_whose_beats_stop_is_failed_within_the_derived_bound(self):
        """Beats stop at T. The last one lands at or before T and lists no later, so every
        listing from T + missed_checks x interval on measures the flag at the threshold or
        more. The throttle lists at the first poll at least an interval after the previous
        listing, so listings are less than an interval and a poll apart, and the first such
        listing is taken before T + (missed_checks + 1) x interval + poll. It runs after the
        query of its poll, so the verdict is formed by the next poll's query:

            verdict <= T + (missed_checks + 1) x interval + 2 x poll  (T + 100 min)

        and never before the flag has really been stale: the last beat plus the threshold,
        less the minute the listing rounds down to. The healthy job beside it is untouched.
        """
        bound = (DEFAULTS["missed_checks"] + 1) * INTERVAL + 2 * POLL
        for phase in self.PHASES:
            stop = 10 * 3600 + 13 * phase  # sweeps more than two listing periods
            stalled = SimJob(
                2, 8, running_from=900 + phase, beats_from=960 + phase, beats_until=stop
            )
            with self.subTest(phase=phase, stop=stop):
                run = simulate([self.healthy(phase), stalled], self.HOURS)
                self.assertEqual(list(run.failed), [2])
                t, reason = run.failed[2]
                self.assertLessEqual(t, stop + bound)
                self.assertGreaterEqual(t, stalled.last_beat(stop) + STALE - 60)
                self.assertIn("min old", reason)


class AnEarlierAttemptsFlagOnTheListingClock(unittest.TestCase):
    """`AStaleFlagLeftByAnEarlierAttempt` pins that a flag left by an earlier attempt of the
    branch does not condemn the attempt now running inside the grace for its first beat.
    Those tests give the verdicts the clock of the query; here they are driven as in
    production, measured at the listing.

    The earlier attempt last beats at 01:29:30, is killed at 01:29:50 without removing its flag
    (as an out-of-memory kill does) and is reported failed from 01:35. The new attempt is
    first seen running at 02:00 and starts beating 35 min after it got its slot: the code
    tarball is fetched and unpacked and law checks the task's inputs before the payload
    starts the heartbeat.
    """

    def attempts(self):
        old = SimJob(
            1,
            7,
            running_from=1700,
            beats_from=1755,
            beats_until=5390,
            running_until=5700,
        )
        new = SimJob(2, 7, running_from=7150, beats_from=7150 + 35 * 60)
        return [old, new]

    def test_measured_at_the_query_the_new_attempt_is_not_condemned(self):
        run = simulate(self.attempts(), hours=6, query_clock=True)
        self.assertEqual(run.failed, {})

    # Which flags may belong to the new attempt is decided on the poll that first saw it
    # running (02:00), not on the listing in force then (01:30): the earlier attempt's flag
    # stamped 01:29 must not count as the new attempt's evidence.
    def test_measured_at_the_listing_the_new_attempt_is_not_condemned(self):
        run = simulate(self.attempts(), hours=6)
        self.assertEqual(run.failed, {})


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

    def test_a_single_missed_check_is_refused(self):
        # a healthy flag reads up to one interval plus its write time old just before its
        # next beat, so a threshold of one interval condemns healthy jobs
        with self.assertRaises(RuntimeError) as caught:
            watchdog_config({"watchdog": {"missed_checks": 1}})
        self.assertIn("missed_checks", str(caught.exception))
        self.assertEqual(
            watchdog_config({"watchdog": {"missed_checks": 2}})["missed_checks"], 2
        )

    def test_a_short_interval_is_refused(self):
        # a beat may take up to Heartbeat.write_timeout_seconds plus the minute a listing
        # rounds down to, and a job its first minutes before the payload beats at all: at
        # an interval of 1 min, a 2 s write already crosses a 2-check threshold
        for minutes in (1, 5, MIN_INTERVAL_MINUTES - 1):
            with self.subTest(minutes=minutes):
                with self.assertRaises(RuntimeError) as caught:
                    watchdog_config({"watchdog": {"interval_minutes": minutes}})
                self.assertIn("interval_minutes", str(caught.exception))
        self.assertEqual(MIN_INTERVAL_MINUTES, 10)
        self.assertEqual(
            watchdog_config({"watchdog": {"interval_minutes": MIN_INTERVAL_MINUTES}})[
                "interval_minutes"
            ],
            MIN_INTERVAL_MINUTES,
        )
        # the floor leaves room for the longest write and the rounding at two checks
        self.assertGreater(
            (2 - 1) * MIN_INTERVAL_MINUTES * 60, Heartbeat.write_timeout_seconds + 60
        )

    def test_every_default_is_documented_in_the_config_reference(self):
        with open(os.path.join(dsprod_repo, "config", "global.yaml")) as f:
            text = f.read()
        for key in DEFAULTS:
            self.assertIn(key, text, f"{key} is not mentioned in config/global.yaml")


class TheJobSideContext(unittest.TestCase):
    """`crab_heartbeat()` is reached only inside a real CRAB job, so every test that does not set
    LAW_CRAB_JOB_NUMBER takes its nullcontext early return -- which is how a NameError in the one
    branch that constructs the Heartbeat survived 187 passing tests and was found by the first
    real job instead."""

    def heartbeat(self, env=None, cfg=None, is_branch=True):
        from dsprod.crab import CrabWorkflow

        task = mock.Mock()
        task._crab_cfg = lambda: {"watchdog": cfg if cfg is not None else {}}
        task.is_branch = lambda: is_branch
        task.branch = 0
        task.task_family = "RunProd"
        task.heartbeat_target = lambda b: mock.Mock(uri=lambda: f"root://x//flags/{b}")
        with mock.patch.dict(os.environ, env or {}, clear=False):
            if env is None:
                os.environ.pop("LAW_CRAB_JOB_NUMBER", None)
            return CrabWorkflow.crab_heartbeat(task)

    def test_a_crab_job_gets_a_real_heartbeat(self):
        from dsprod.watchdog import Heartbeat

        hb = self.heartbeat(env={"LAW_CRAB_JOB_NUMBER": "1"})
        self.assertIsInstance(hb, Heartbeat)
        self.assertEqual(hb.uri, "root://x//flags/0")
        self.assertEqual(hb.interval, 30 * 60)

    def test_the_interval_comes_from_the_configuration(self):
        hb = self.heartbeat(
            env={"LAW_CRAB_JOB_NUMBER": "1"}, cfg={"interval_minutes": 15}
        )
        self.assertEqual(hb.interval, 15 * 60)

    def test_anything_that_is_not_a_crab_job_writes_nothing(self):
        import contextlib

        for kwargs in (
            {},  # no LAW_CRAB_JOB_NUMBER: local or htcondor
            {"env": {"LAW_CRAB_JOB_NUMBER": "1"}, "cfg": False},  # switched off
            {
                "env": {"LAW_CRAB_JOB_NUMBER": "1"},
                "is_branch": False,
            },  # the workflow itself
        ):
            hb = self.heartbeat(**kwargs)
            self.assertIsInstance(hb, contextlib.nullcontext, kwargs)

    def test_a_watchdog_setting_crab_would_refuse_does_not_reach_other_backends(self):
        """The CRAB driver refuses these settings before submitting; a local or HTCondor run never
        builds that driver, so its payloads must not be the ones to raise on them."""
        import contextlib

        for cfg in ({"interval_minutes": 5}, {"missed_checks": 1}):
            hb = self.heartbeat(cfg=cfg)
            self.assertIsInstance(hb, contextlib.nullcontext, cfg)
            with self.assertRaises(RuntimeError, msg=cfg):
                self.heartbeat(env={"LAW_CRAB_JOB_NUMBER": "1"}, cfg=cfg)

    def test_the_context_can_be_entered_and_left(self):
        """Exercises the thread start/stop and the flag removal, with the storage stubbed."""
        hb = self.heartbeat(
            env={"LAW_CRAB_JOB_NUMBER": "1"}, cfg={"interval_minutes": 60}
        )
        with mock.patch("dsprod.watchdog.gfal_copy") as copy, mock.patch(
            "dsprod.watchdog.gfal_rm"
        ) as rm:
            with hb:
                pass
        self.assertGreaterEqual(copy.call_count, 1, "no beat was written")
        self.assertTrue(
            copy.call_args.kwargs.get("force"), "the beat must overwrite in place"
        )
        rm.assert_called_once()


class TheJobSideHeartbeat(unittest.TestCase):
    """The storage is stubbed; what is checked is how long a beat or the removal may take,
    and how soon a failed beat is tried again."""

    URI = "root://x//flags/heartbeat/RunProd_x/7"

    def run_heartbeat(self, rm_effect=None):
        log = []
        hb = Heartbeat(self.URI, 3600, voms_token="/tmp/proxy", log=log.append)
        with mock.patch("dsprod.watchdog.gfal_copy") as copy, mock.patch(
            "dsprod.watchdog.gfal_rm", side_effect=rm_effect
        ) as rm:
            with hb:
                pass
        return copy, rm, log

    def test_a_beat_is_bounded(self):
        """gfal-copy's own limit here is two hours; a beat hanging that long would hold back
        every later one. One write may take at most write_timeout_seconds, and never longer
        than the interval."""
        copy, _, _ = self.run_heartbeat()
        self.assertEqual(Heartbeat.write_timeout_seconds, 300)
        self.assertEqual(copy.call_args.kwargs.get("timeout"), 300)
        with mock.patch("dsprod.watchdog.gfal_copy") as copy:
            Heartbeat(self.URI, 90.7)._write()
        self.assertEqual(copy.call_args.kwargs.get("timeout"), 90)

    def test_the_removal_is_bounded_like_a_beat(self):
        """A hanging delete would hold a job whose payload has already finished."""
        _, rm, log = self.run_heartbeat()
        rm.assert_called_once_with(
            self.URI, voms_token="/tmp/proxy", verbose=0, timeout=300
        )
        self.assertEqual(log, [])

    def test_a_failed_removal_is_logged_and_never_reaches_the_payload(self):
        _, rm, log = self.run_heartbeat(rm_effect=RuntimeError("storage down"))
        rm.assert_called_once()
        self.assertTrue(any("could not remove" in m for m in log), log)

    def run_loop(self, effects, n_waits):
        """Run the beat loop over `effects` (one per write) and return what it waited."""
        hb = Heartbeat(self.URI, 1800)
        waits = []

        class Stop:
            def wait(self, seconds):
                waits.append(seconds)
                return len(waits) >= n_waits

        hb._stop = Stop()
        with mock.patch("dsprod.watchdog.gfal_copy", side_effect=effects):
            hb._loop()
        return waits

    def test_a_failed_beat_is_tried_again_soon(self):
        """The driver tolerates `missed_checks` intervals; waiting a whole interval after a
        failed beat would make one failure look like a stall."""
        waits = self.run_loop([RuntimeError("down"), None], 2)
        self.assertEqual(Heartbeat.retry_seconds, 60)
        self.assertEqual(waits, [60, 1800])

    def test_repeated_failures_back_off_up_to_a_quarter_of_an_interval(self):
        """Thousands of jobs retrying every minute would hammer a storage that is already
        failing; a success starts over."""
        effects = [RuntimeError("down")] * 5 + [None, RuntimeError("down")]
        waits = self.run_loop(effects, 7)
        self.assertEqual(waits, [60, 120, 240, 450, 450, 1800, 60])


class WhereTheFlagsLive(unittest.TestCase):
    """`fs_watchdog` is a separate endpoint so the heartbeat load, and the heartbeat's own
    availability, are independent of the storage the products go to."""

    def setUp(self):
        from dsprod import tasks

        self.tasks = tasks
        tasks._fs_cache.clear()
        self.addCleanup(tasks._fs_cache.clear)

    @staticmethod
    def base_of(fs):
        # law's LocalFileSystem.base is a str; the remote one's is a list of uris
        base = fs.base if isinstance(fs.base, str) else fs.base[0]
        return base.rstrip("/")

    def _fs(self, cfg):
        with mock.patch("dsprod.tasks.get_global", return_value=cfg):
            return self.tasks.get_watchdog_fs()

    def test_it_is_used_when_configured(self):
        fs = self._fs({"fs_default": "/products", "fs_watchdog": "/beats"})
        self.assertEqual(self.base_of(fs), "/beats")

    def test_it_falls_back_to_the_products_file_system(self):
        fs = self._fs({"fs_default": "/products"})
        self.assertEqual(self.base_of(fs), "/products")

    def test_the_products_file_system_is_untouched_by_it(self):
        with mock.patch(
            "dsprod.tasks.get_global",
            return_value={"fs_default": "/products", "fs_watchdog": "/beats"},
        ):
            self.assertEqual(self.base_of(self.tasks.get_fs()), "/products")

    def test_a_missing_key_with_no_fallback_says_which_key(self):
        with mock.patch("dsprod.tasks.get_global", return_value={}):
            with self.assertRaises(RuntimeError) as caught:
                self.tasks.get_fs("fs_watchdog")
            self.assertIn("fs_watchdog", str(caught.exception))

    def test_each_file_system_is_built_once(self):
        cfg = {"fs_default": "/products", "fs_watchdog": "/beats"}
        with mock.patch("dsprod.tasks.get_global", return_value=cfg):
            self.assertIs(self.tasks.get_watchdog_fs(), self.tasks.get_watchdog_fs())
            self.assertIsNot(self.tasks.get_watchdog_fs(), self.tasks.get_fs())


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

    def test_the_production_path_measures_ages_at_the_listing(self):
        """Neither entry point gives the watchdog a time of its own: `crab_poll_callback`
        lists through its throttle and `_apply_watchdog` asks for the verdicts on the poll's
        fresh status. A flag beaten at 11:30 and listed at 12:00:50 is 31 min old at that
        listing; the query at 12:30:50, the last poll before the next listing, would read it
        61 min old and fail a healthy job. The flag beaten at 10:59 is stale either way.
        """
        from dsprod.crab import CrabWorkflow

        w = StallWatchdog("root://x//flags", watchdog_config({}))
        w.publish = lambda msg: None
        job_map = jobs((1, 7), (2, 8))
        workflow = mock.Mock(spec=CrabWorkflow)
        workflow._dsprod_job_manager = None
        workflow._crab_kerberos_update = lambda: None
        workflow._watchdog_refresh = None
        workflow.job_watchdog.return_value = w
        workflow.workflow_proxy = types.SimpleNamespace(
            job_data=types.SimpleNamespace(jobs=job_map)
        )
        clock = Clock(NOW + datetime.timedelta(seconds=50))
        flags = [Flag(7, age_minutes=30), Flag(8, age_minutes=61)]
        result = status(1, 2)
        with mock.patch("dsprod.watchdog.datetime", clock), mock.patch.object(
            tools, "datetime", clock
        ), mock.patch("dsprod.watchdog.gfal_ls_safe", return_value=flags) as ls:
            self.assertTrue(CrabWorkflow.crab_poll_callback(workflow, mock.Mock()))
            ls.assert_called_once()
            self.assertEqual(w._listed_at, clock.time)
            # both jobs watched long enough that the grace for a first beat is over
            prime(w, job_map)
            clock.time = at(30) + datetime.timedelta(seconds=50)
            self.manager(w)._apply_watchdog(result)
        self.assertEqual(result[(1, TASK, "/proj")]["status"], "running")
        self.assertEqual(result[(2, TASK, "/proj")]["status"], "failed")
        self.assertIn("62 min old", result[(2, TASK, "/proj")]["error"])


if __name__ == "__main__":
    unittest.main()
