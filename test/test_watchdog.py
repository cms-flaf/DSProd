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
            env={"LAW_CRAB_JOB_NUMBER": "1"}, cfg={"interval_minutes": 5}
        )
        self.assertEqual(hb.interval, 5 * 60)

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


if __name__ == "__main__":
    unittest.main()
