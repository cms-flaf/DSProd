#!/usr/bin/env python3
"""A skipped first submission round must not leave law's poll loop waiting for a job that is not there.

law's `poll()` fixes the job count (`len(job_data)`, backlog included) once at its start, and law's
`submit()` drops from `unsubmitted_jobs` every job whose branches already exist. On a fresh run such a
job is also in `jobs`: luigi's scheduling (`process_resources` -> `_can_skip_job`) recorded it there as
finished, while `_run_impl` put every chunk in the backlog -- so it is counted twice until that drop.
law calls `submit()` right before `poll()`; DSProd skips that round when law's own tree cannot be read,
and the drop then happens inside the loop, which waits for one job more than there are. FLAF's CRAB CI
hung that way on 2026-10-17 (`all: 8 ... finished: 7`).

Everything law does here is law's own: the proxy's real `run()`, law's `_run_impl()`, `submit()` and
`poll()`. Replaced: `_submit_group` (no CRAB task is created), the batch system's answers, the probe
of law's tree, and the parts of a task the proxy reads.
"""

import contextlib
import os
import shutil
import sys
import tempfile
import unittest
from collections import OrderedDict
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

import law  # noqa: E402
from law.job.base import BaseJobManager  # noqa: E402
from law.job.dashboard import NoJobDashboard  # noqa: E402
from law.workflow.remote import JobData  # noqa: E402

law.contrib.load("cms")

from dsprod.crab import DSProdCrabJobFileFactory, DSProdCrabWorkflowProxy  # noqa: E402

FINISHED = BaseJobManager.FINISHED
RUNNING = BaseJobManager.RUNNING

LAW_TREE = "/eos/.../law/contrib/cms/crab/crab_wrapper.sh"


class ScriptedJobManager(BaseJobManager):
    """law's job-manager base with the batch system replaced: every job answers FINISHED."""

    job_grouping_submit = True

    def query(self, job_id, **kwargs):
        return self.job_status_dict(job_id=job_id, status=FINISHED, code=0)

    def submit(self, *args, **kwargs):
        raise AssertionError("jobs reach the batch system through _submit_group only")

    def cancel(self, *args, **kwargs):
        raise AssertionError("nothing is cancelled here")

    def cleanup(self, *args, **kwargs):
        raise AssertionError("nothing is cleaned up here")


class FakeWorkflowTask:
    """The workflow task behind the proxy, as law's proxy, run() and poll() read it: law's
    parameter defaults, no wait between polls, one local output file per branch."""

    dump_intermediate_job_data = True
    tasks_per_job = 1
    submission_threads = 1
    parallel_jobs = law.NO_INT
    poll_interval = 0.0
    poll_fails = 5
    walltime = law.NO_FLOAT
    acceptance = 1.0
    tolerance = 0.0
    retries = 5
    no_poll = False
    shuffle_jobs = False
    append_retry_jobs = False
    clear_logs = False
    ignore_submission = False
    cancel_jobs = False
    cleanup_jobs = False
    align_polling_status_line = False
    check_unreachable_acceptance = False
    reset_branch_map_before_run = False
    cache_branch_map = True
    crab_job_kwargs_submit = {}
    crab_job_kwargs_query = {}

    #: a poll loop that runs longer than this is a test failure, not a hang
    max_polls = 25

    def __init__(self, workdir, n_branches):
        self.task_id = f"FakeWorkflowTask__{os.path.basename(workdir)}"
        self.workdir = workdir
        self.branch_map = OrderedDict((b, b) for b in range(n_branches))
        self.messages = []
        self.n_polls = 0
        self.jobs_file = law.LocalFileTarget(os.path.join(workdir, "jobs.json"))
        self.collection = law.TargetCollection(
            OrderedDict(
                (b, law.LocalFileTarget(self.output_path(b))) for b in self.branch_map
            )
        )

    def output_path(self, branch):
        return os.path.join(self.workdir, "outputs", f"branch_{branch}.root")

    def produce(self, branches):
        for b in branches:
            os.makedirs(os.path.dirname(self.output_path(b)), exist_ok=True)
            open(self.output_path(b), "w").close()

    def get_branch_map(self):
        return self.branch_map

    def outputs(self):
        return {"jobs": self.jobs_file, "collection": self.collection}

    def is_controlling_remote_jobs(self):
        return False

    def get_task_family(self):
        return "FakeWorkflowTask"

    def _crab_cfg(self):
        return {}

    def publish_message(self, msg, *args, **kwargs):
        self.messages.append(str(msg))

    def publish_progress(self, *args, **kwargs):
        pass

    def decrease_running_resources(self, *args, **kwargs):
        pass

    def _handle_scheduler_messages(self):
        pass

    def forward_dashboard_event(self, *args, **kwargs):
        pass

    def modify_polling_status_line(self, status_line):
        return status_line

    def set_tracking_url(self, url):
        pass

    def create_job_dashboard(self):
        return None

    def crab_create_job_manager(self, **kwargs):
        return ScriptedJobManager(**kwargs)

    def crab_create_job_file_factory(self, **kwargs):
        path = os.path.join(self.workdir, "job_files")
        os.makedirs(path, exist_ok=True)
        return DSProdCrabJobFileFactory(dir=path)

    def crab_workflow_run_context(self):
        return contextlib.nullcontext()

    def crab_dump_intermediate_job_data(self):
        return self.dump_intermediate_job_data

    def crab_job_resources(self, job_num, branches):
        return {}

    def crab_post_submit_delay(self):
        return 0

    def crab_check_job_completeness(self):
        return False

    def crab_check_job_completeness_delay(self):
        return 0

    def crab_poll_callback(self, poll_data):
        self.n_polls += 1
        if self.n_polls > self.max_polls:
            raise AssertionError(f"the poll loop did not end in {self.max_polls} polls")
        return True

    def crab_post_poll_callback(self, success, duration):
        return None

    def crab_destination_info(self, info):
        return info


class TheJobCountTheLoopWaitsFor(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp(prefix="dsprod_skipped_round_")
        self.addCleanup(shutil.rmtree, self.workdir, ignore_errors=True)
        self.task = FakeWorkflowTask(self.workdir, 3)
        self.proxy = DSProdCrabWorkflowProxy(task=self.task)
        # what law's run() sets up from the task's output() before it submits or polls
        self.proxy.dashboard = NoJobDashboard()
        self.proxy._cached_output = self.task.outputs()
        self.proxy._job_manager_setup_kwargs = {}
        #: one list of job numbers per round that reached the batch system
        self.submitted = []
        for patcher in (
            mock.patch.object(
                DSProdCrabWorkflowProxy, "setup_job_manager", return_value={}
            ),
            mock.patch.object(
                DSProdCrabWorkflowProxy, "_submit_group", self._submit_group
            ),
            mock.patch.object(
                DSProdCrabJobFileFactory, "law_source_error", return_value="ENOENT"
            ),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def _submit_group(self, submit_jobs, **kwargs):
        nums = list(submit_jobs)
        self.submitted.append(nums)
        ids = [f"{num}.{len(self.submitted)}" for num in nums]
        data = OrderedDict(
            (num, {"job": "job.cfg", "config": None, "log": None}) for num in nums
        )
        return ids, data

    def run_with_tree(self, *answers):
        """Run the workflow; the probe of law's tree answers `answers` in turn, then None."""
        answers = list(answers)
        with mock.patch.object(
            DSProdCrabJobFileFactory,
            "missing_law_source",
            side_effect=lambda **kwargs: answers.pop(0) if answers else None,
        ):
            self.proxy.run()

    def statuses(self):
        return {num: data["status"] for num, data in self.proxy.job_data.jobs.items()}

    def test_a_skipped_first_round_with_a_complete_job_ends(self):
        self.task.produce([0])
        # luigi's scheduling: law records job 1, whose branch exists, as finished
        self.proxy.process_resources()
        self.run_with_tree(LAW_TREE)
        self.assertEqual(
            sum("skipping this submission round" in m for m in self.task.messages), 1
        )
        self.assertEqual(self.submitted, [[2, 3]])
        self.assertEqual(self.proxy.job_data.unsubmitted_jobs, {})
        self.assertEqual(self.statuses(), {1: FINISHED, 2: FINISHED, 3: FINISHED})

    def test_a_readable_tree_is_unchanged(self):
        self.task.produce([0])
        self.proxy.process_resources()
        self.run_with_tree()
        self.assertEqual(self.submitted, [[2, 3]])
        self.assertEqual(self.statuses(), {1: FINISHED, 2: FINISHED, 3: FINISHED})

    def test_a_resumed_backlog_with_a_complete_job_ends(self):
        data = JobData()
        data.jobs[1] = JobData.job_data(
            job_id="r1", branches=[0], status=FINISHED, code=0
        )
        data.unsubmitted_jobs[2] = [1]
        data.unsubmitted_jobs[3] = [2]
        self.task.jobs_file.dump(data, formatter="json", indent=4)
        self.task.produce([0, 1])
        self.proxy.process_resources()
        self.run_with_tree(LAW_TREE)
        self.assertEqual(self.submitted, [[3]])
        self.assertEqual(self.proxy.job_data.unsubmitted_jobs, {})
        self.assertEqual(set(self.statuses().values()), {FINISHED})


if __name__ == "__main__":
    unittest.main()
