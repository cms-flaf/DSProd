"""What the driver-staleness alarm must report, and what it must stay quiet about.

The alarm runs from acron every 30 min, so a false report is expensive in a different way than a
missed one: an operator who gets mail from a healthy production stops reading the mail. The cases
below are the ones the live Run3_2023BPix production actually produces -- a poll gap of half an
hour, a dump caught mid-write, a production that has finished, retries parked as unsubmitted by
the CRAB wave gate, a handful of failures left behind at the tail, a second process merging the
part of the production that is already done, and a stall that lasts for days and must be reported
once.
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import time
import unittest

from law.job.base import BaseJobManager
from law.workflow.remote import JobData

run_tools = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "run_tools"
)
if run_tools not in sys.path:
    sys.path.insert(0, run_tools)

import check_driver_alive as alarm


def job_dump(jobs=None, unsubmitted=None):
    """A law job-data dump, built by law itself so its schema cannot drift from the real one."""
    data = JobData(tasks_per_job=1)
    data.jobs = {
        num: JobData.job_data(job_id=f"{num}.0", branches=[num], status=status)
        for num, status in enumerate(jobs or [BaseJobManager.RUNNING])
    }
    data.unsubmitted_jobs = dict(unsubmitted or {})
    return json.dumps(data)


class Area:
    """A production area holding only what the alarm reads."""

    def __init__(self, root):
        self.root = root
        self.state = os.path.join(root, "alarm_state.json")

    def add_crab_project(self, name="crab_RunProd_9f3c1a20"):
        path = os.path.join(self.root, "data", "jobs", "tmpq1w2e3", name)
        os.makedirs(path)
        return path

    def add_dump(
        self,
        store="RunProd/Run3_2023BPix_XHHbbWW",
        name="crab_jobs_0To4800.json",
        age_minutes=0.0,
        content=None,
    ):
        path = os.path.join(self.root, "data", *store.split("/"), name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content if content is not None else job_dump())
        stamp = time.time() - age_minutes * 60.0
        os.utime(path, (stamp, stamp))
        return path

    def run(self, threshold=None):
        argv = ["--area", self.root, "--state-file", self.state]
        if threshold is not None:
            argv += ["--threshold-minutes", str(threshold)]
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = alarm.main(argv)
        return code, out.getvalue() + err.getvalue()


class AlarmTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.area = Area(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()


class TestHealthyProduction(AlarmTestCase):
    def test_a_polling_driver_says_nothing_at_all(self):
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=2)
        self.assertEqual(self.area.run(), (0, ""))

    def test_the_worst_measured_poll_gap_is_not_a_stall(self):
        # 32.3 min was the largest gap seen in the live production: submission is silent for
        # 5-13 min and a failed status query skips the dump entirely
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=32.3)
        self.assertEqual(self.area.run(), (0, ""))

    def test_twice_the_poll_interval_would_have_alarmed_on_that_gap(self):
        # why the default threshold is 45 min and not 2 x poll_interval
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=32.3)
        code, text = self.area.run(threshold=10)
        self.assertEqual(code, 1)
        self.assertIn("no driver has polled", text)


class TestNothingToDrive(AlarmTestCase):
    def test_an_area_that_never_submitted_to_crab_is_silent(self):
        # a fresh checkout, or a local-only production: an ancient dump there means nothing
        self.area.add_dump(age_minutes=3 * 60)
        self.assertEqual(self.area.run(), (0, ""))

    def test_a_finished_production_is_silent_forever(self):
        # its last dump only gets older; alarming on it would mail the operator every 30 min
        self.area.add_crab_project()
        self.area.add_dump(
            age_minutes=3 * 60,
            content=job_dump(jobs=[BaseJobManager.FINISHED, BaseJobManager.FINISHED]),
        )
        self.assertEqual(self.area.run(), (0, ""))

    def test_retries_parked_as_unsubmitted_are_still_work(self):
        # the CRAB wave gate moves retries back to unsubmitted_jobs, so every job can read as
        # finished while a wave is still owed (median parking measured at 11.35 h)
        self.area.add_crab_project()
        self.area.add_dump(
            age_minutes=3 * 60,
            content=job_dump(jobs=[BaseJobManager.FINISHED], unsubmitted={"7": [7]}),
        )
        code, text = self.area.run()
        self.assertEqual(code, 1)
        self.assertIn("no driver has polled", text)


class TestStallReporting(AlarmTestCase):
    def test_a_stall_names_the_area_and_the_dump(self):
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=27 * 60)
        code, text = self.area.run()
        self.assertEqual(code, 1)
        self.assertIn("27h00m", text)
        self.assertIn(self.tmp.name, text)
        self.assertIn("crab_jobs_0To4800.json", text)
        self.assertIn("drive.sh", text)

    def test_one_stall_is_reported_once(self):
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=3 * 60)
        self.assertEqual(self.area.run()[0], 1)
        self.assertEqual(self.area.run(), (0, ""))

    def test_a_poll_followed_by_a_new_stall_is_reported_again(self):
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=3 * 60)
        self.assertEqual(self.area.run()[0], 1)
        # the driver came back, polled once, and died again
        self.area.add_dump(age_minutes=2 * 60)
        self.assertEqual(self.area.run()[0], 1)

    def test_crab_projects_without_any_dump_are_reported_once(self):
        self.area.add_crab_project()
        code, text = self.area.run()
        self.assertEqual(code, 1)
        self.assertIn("no driver has recorded a poll", text)
        # nothing about this area changes on its own, so acron would mail it 48 times a day
        for _ in range(3):
            self.assertEqual(self.area.run(), (0, ""))

    def test_a_tail_of_failed_jobs_with_no_driver_is_reported_once(self):
        # law rebuilds `_job_retries` in every process and reads nothing back from the dump, so a
        # restart resubmits these branches; staying quiet here is staying quiet on the 27 h gap
        # this alarm exists for -- and a handful of fast failures at the tail is what a black-hole
        # site produces
        self.area.add_crab_project()
        self.area.add_dump(
            age_minutes=27 * 60,
            content=job_dump(
                jobs=[BaseJobManager.FINISHED] * 4 + [BaseJobManager.FAILED]
            ),
        )
        code, text = self.area.run()
        self.assertEqual(code, 1)
        self.assertIn("no driver has polled", text)
        # a production that really has ended keeps that dump mtime for good, so it says this once
        self.assertEqual(self.area.run(), (0, ""))


class TestUnreadableDump(AlarmTestCase):
    def test_a_dump_caught_mid_write_is_not_a_stall(self):
        # law rewrites the dump in place on every poll, so a read can catch it truncated
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=2, content='{"jobs": {"1": {"stat')
        self.assertEqual(self.area.run(), (0, ""))

    def test_an_old_unreadable_dump_still_alarms(self):
        # nothing proves the production finished, and silence is the worse mistake here
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=3 * 60, content='{"jobs": {"1": {"stat')
        self.assertEqual(self.area.run()[0], 1)


class TestUnwritableState(AlarmTestCase):
    """The healthy path must be silent under every condition, or the mail stops being read."""

    def setUp(self):
        super().setUp()
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=3 * 60)
        self.assertEqual(self.area.run()[0], 1)
        os.chmod(self.area.state, 0o400)

    def tearDown(self):
        os.chmod(self.area.state, 0o600)
        super().tearDown()

    def test_a_recovered_driver_says_nothing_even_if_the_state_cannot_be_cleared(self):
        self.area.add_dump(age_minutes=2)
        self.assertEqual(self.area.run(), (0, ""))

    def test_a_report_says_when_it_may_repeat(self):
        self.area.add_dump(age_minutes=2 * 60)
        code, text = self.area.run()
        self.assertEqual(code, 1)
        self.assertIn("may repeat", text)


class TestStateFileWithoutADirectory(AlarmTestCase):
    def test_a_bare_state_file_name_still_suppresses_the_repeat(self):
        # os.path.dirname("alarm_state.json") is "", and makedirs("") raises: the state was never
        # written, so the stall was reported again on every run
        self.area.add_crab_project()
        self.area.add_dump(age_minutes=3 * 60)
        argv = ["--area", self.tmp.name, "--state-file", "alarm_state.json"]
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                first, second = alarm.main(argv), alarm.main(argv)
        finally:
            os.chdir(cwd)
        self.assertEqual((first, second), (1, 0))
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "alarm_state.json")))


class TestNewestDumpWins(AlarmTestCase):
    def test_a_fresh_dump_elsewhere_in_the_area_proves_the_driver_is_alive(self):
        # a production writes one dump per store directory, and old ones are never removed
        self.area.add_crab_project()
        self.area.add_dump(store="RunProd/old_era", age_minutes=40 * 60)
        self.area.add_dump(store="RunProd/current_era", age_minutes=3)
        self.assertEqual(self.area.run(), (0, ""))

    def test_the_newest_dump_wins_whatever_order_the_glob_returns(self):
        # glob does not sort, so "take the first match" passes or fails by directory order
        old = self.area.add_dump(store="RunProd/a_era", age_minutes=40 * 60)
        fresh = self.area.add_dump(store="RunProd/b_era", age_minutes=3)
        for order in ([old, fresh], [fresh, old]):
            found, _ = alarm.newest_dump_with_work(
                alarm.job_dumps(
                    self.tmp.name, glob_fn=lambda pattern, order=order: list(order)
                )
            )
            self.assertEqual(found, fresh)

    def test_a_settled_older_dump_does_not_excuse_a_stalled_newer_one(self):
        self.area.add_crab_project()
        self.area.add_dump(
            store="NanoMergeTask/done_era",
            age_minutes=40 * 60,
            content=job_dump(jobs=[BaseJobManager.FINISHED]),
        )
        self.area.add_dump(store="RunProd/current_era", age_minutes=3 * 60)
        self.assertEqual(self.area.run()[0], 1)


class TestASecondWorkflowInTheArea(AlarmTestCase):
    """A merge run leaves a finished dump behind, and it must not answer for the whole area.

    Merging the finished part of a production from a second process is a supported procedure
    (`docs/operations/long-productions.md`), so the newest file under `data/*/*/crab_jobs_*.json`
    is routinely a `NanoMergeTask` dump with every job finished. Keyed on the newest dump alone,
    that file silenced the alarm for good -- and "the RunProd driver dies before the merge run
    finishes" is the ordinary case in an area whose driver dies roughly daily.
    """

    def test_a_finished_merge_dump_does_not_excuse_a_stalled_production(self):
        self.area.add_crab_project()
        self.area.add_dump(store="RunProd/current_era", age_minutes=3 * 60)
        self.area.add_dump(
            store="NanoMergeTask/current_era",
            name="crab_jobs_0To79_96To175.json",
            age_minutes=1,
            content=job_dump(jobs=[BaseJobManager.FINISHED] * 10),
        )
        code, text = self.area.run()
        self.assertEqual(code, 1)
        self.assertIn("no driver has polled RunProd", text)
        # ... and it names the stalled workflow's dump, not the fresh one it ignored
        self.assertIn("crab_jobs_0To4800.json", text)
        self.assertNotIn("crab_jobs_0To79_96To175.json", text)

    def test_a_workflow_being_polled_still_excuses_the_area(self):
        # Deliberate, and the reason the newest dump with work is taken rather than the oldest: an
        # area accumulates dumps from selections nobody drives any more (law never removes them),
        # and any of those with a `running` job left in it would otherwise mail forever. The cost
        # is that a gap in one workflow is masked while another is polled -- bounded by that run,
        # because a finished dump stops excusing anything (the test above).
        self.area.add_crab_project()
        self.area.add_dump(store="RunProd/current_era", age_minutes=3 * 60)
        self.area.add_dump(
            store="NanoMergeTask/current_era",
            name="crab_jobs_0To79.json",
            age_minutes=2,
        )
        self.assertEqual(self.area.run(), (0, ""))


if __name__ == "__main__":
    unittest.main()
