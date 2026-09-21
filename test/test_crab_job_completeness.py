#!/usr/bin/env python3
"""A CRAB job is finished when its products exist, not when CRAB says so.

CRAB parks a job in `transferring` between the payload exiting and the post-job classifying it,
and it parks a payload that exited non-zero there too. law maps that state to FINISHED whenever
transfers are skipped -- which DSProd does, since these jobs stage their own products -- so a poll
landing inside that window reads a failed job as finished, writes it off with `dummy_job_id` and
never queries it again. On 2026-09-18 one poll harvested 113 such jobs and the driver reported
`finished: 113` for a production that had written nothing at all: no staging/, no produced/, no
nanoAOD_v12/ on fs_default.

`crab_check_job_completeness()` closes that door, and these tests hold it shut from three sides:
a job whose products are missing must be demoted to FAILED and retried; a job whose products are
there must be checked ONCE for the whole run, not on every poll; and -- the mirror failure, which
a first version of this file could not see because it mocked `complete()` -- a job whose record
was written since the last directory listing must NOT be demoted. `exists()` answers an unknown
file from a cached listing of its directory (600 s, against a 5 min poll), so without dropping
those listings the check turns "finished" into "failed" for jobs that did the work.

The storage here is therefore the real `GFALFileInterface` with its real cache, over a fake
listing call: what is under test is which answer the cache gives, so the cache must be the real
one.
"""

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

# `Task.to_abs` resolves the setup path against $ANALYSIS_PATH, and the checkout is the area
os.environ["ANALYSIS_PATH"] = dsprod_repo

import law  # noqa: E402
from law.job.dashboard import NoJobDashboard  # noqa: E402

law.contrib.load("cms")

from dsprod import law_gfal  # noqa: E402
from dsprod.crab import CrabWorkflow, DSProdCrabWorkflowProxy  # noqa: E402
from dsprod.law_wlcg import WLCGFileSystem  # noqa: E402
from dsprod.tasks import RunProd  # noqa: E402

SETUP = "models/X_HH/setups/Run3_XHHbbWW.yaml"

_data_dir = None
_sandbox_patcher = None


def setUpModule():
    # nothing here writes job data, but a stray write must never land in the production area
    global _data_dir, _sandbox_patcher
    _data_dir = tempfile.mkdtemp(prefix="dsprod_completeness_test_")
    os.environ["ANALYSIS_DATA_PATH"] = _data_dir
    # building the proxy runs the sandbox pre-flight, which needs cvmfs; nothing under test does
    _sandbox_patcher = mock.patch.object(
        law.cms.CrabJobManager,
        "cmssw_env",
        new_callable=mock.PropertyMock,
        return_value={"PATH": os.environ.get("PATH", "")},
    )
    _sandbox_patcher.start()


def tearDownModule():
    _sandbox_patcher.stop()
    shutil.rmtree(_data_dir, ignore_errors=True)


def state(status, code=None, error=None):
    """One job's entry in a CRAB query response, as law reads it."""
    return {"status": status, "code": code, "error": error, "extra": {}}


class Entry:
    def __init__(self, name):
        self.name = name


class FakeStorage:
    """What the endpoint would answer, keyed by directory URI.

    Only the `gfal-ls` call is faked. The cache, the "unknown file in a known directory is absent"
    shortcut and the ancestor bookkeeping above it are the real ones, because they are what decides
    the verdict.
    """

    def __init__(self):
        self.dirs = {}
        self.listings = 0

    def add(self, uri):
        while "/" in uri.rstrip("/")[len("davs://") :]:
            parent, _, name = uri.rpartition("/")
            if not name:
                break
            self.dirs.setdefault(parent, set()).add(name)
            uri = parent
            if uri.endswith("//"):
                break

    def ls(self, uri, **kwargs):
        self.listings += 1
        if uri not in self.dirs:
            return None  # gfal's "no such file or directory"
        return [Entry(name) for name in sorted(self.dirs[uri])]


class PollHarness:
    """Drives law's real poll loop over scripted CRAB responses and real storage answers.

    Nothing about completeness is replaced: the branch's own `complete()` runs, against the real
    `GFALFileInterface` and its real cache, over a fake `gfal-ls`. Only the CRAB query, the
    credential setup (a runner has no VOMS proxy) and the writes a test must not make are doubles.
    """

    def __init__(
        self, case, responses, before_query=None, acceptance=1.0, tolerance=0.0
    ):
        self.case = case
        self.responses = list(responses)
        #: poll index -> what happened on the storage since the previous poll finished. It fires
        #: BEFORE that poll's status is read, which is the order the grid works in: a job runs and
        #: writes between two polls, and the poll that reports it finished is the next one
        self.before_query = dict(before_query or {})
        self.queries = 0
        #: branch -> how often law asked whether it is complete
        self.complete_calls = {}
        self.storage = FakeStorage()

        with mock.patch.object(
            law_gfal, "get_voms_proxy_info", return_value={"path": "/tmp/token"}
        ):
            self.fs = WLCGFileSystem(base="davs://fake/base")

        for patcher in (
            mock.patch.object(law_gfal, "gfal_ls_checked", side_effect=self.storage.ls),
            mock.patch("dsprod.tasks.get_fs", return_value=self.fs),
        ):
            patcher.start()
            case.addCleanup(patcher.stop)

        with mock.patch.object(CrabWorkflow, "_crab_cfg", return_value={}):
            self.task = RunProd(
                setup=SETUP,
                eras=("Run3_2023BPix",),
                points=("*_M-250",),
                workflow="crab",
                acceptance=acceptance,
                tolerance=tolerance,
                retries=0,
                poll_interval=0.0,
            )
            self.proxy = DSProdCrabWorkflowProxy(task=self.task)
        self.proxy.dashboard = NoJobDashboard()
        # a fresh submission, as a production is: law has a separate rule for the first poll of a
        # RESUMED one (`_submitted and i == 0`), where a finished job with no outputs is retried as
        # "initially missing task outputs" -- a guard that covers exactly one iteration of one case
        # and is why this incident happened on a freshly submitted task
        self.proxy._submitted = False
        self.proxy.dump_job_data = lambda: None
        # law ORs accepted branches into this set in place
        self.proxy._existing_branches = set()

    def job(self, job_num, branch):
        self.proxy.job_data.jobs[job_num] = self.proxy.job_data_cls.job_data(
            branches=[branch],
            # a tuple, not the list a JSON round trip gives: law keys its per-job state map on it
            job_id=("crab", job_num, "proj"),
        )

    def produce(self, branch):
        """Put a branch's products on the fake storage, as a job that ran would."""
        for target in self.task.as_branch(branch).output().values():
            self.storage.add(self.fs.file_interface.uri(target.path))

    def _query(self, job_ids, **kwargs):
        action = self.before_query.get(self.queries)
        if action is not None:
            action()
        response = self.responses[min(self.queries, len(self.responses) - 1)]
        self.queries += 1
        return OrderedDict(
            (job_id, response[job_id[1]]) for job_id in job_ids if job_id[1] in response
        )

    def run(self):
        real_complete = RunProd.complete

        def spy(task_self):
            branch = getattr(task_self, "branch", None)
            if branch is not None and branch >= 0:
                self.complete_calls[branch] = self.complete_calls.get(branch, 0) + 1
            return real_complete(task_self)

        with mock.patch.object(
            DSProdCrabWorkflowProxy, "setup_job_manager", return_value={}
        ), mock.patch.object(
            type(self.proxy.job_manager), "query_group", side_effect=self._query
        ), mock.patch.object(
            type(self.proxy.job_manager), "query_batch", side_effect=self._query
        ), mock.patch.object(
            RunProd, "complete", autospec=True, side_effect=spy
        ), mock.patch.object(
            DSProdCrabWorkflowProxy,
            "_get_existing_branches",
            side_effect=lambda *a, **k: self.proxy._existing_branches,
        ), mock.patch.object(
            DSProdCrabWorkflowProxy, "submit", return_value=OrderedDict()
        ), mock.patch.object(
            CrabWorkflow, "crab_poll_callback", return_value=True
        ), mock.patch.object(
            # luigi attaches `scheduler_messages` only while it runs a task, and the loop reads it
            # once per iteration
            type(self.task),
            "_handle_scheduler_messages",
        ), mock.patch.object(
            type(self.task), "publish_message"
        ):
            self.proxy.poll()

    def status(self, job_num):
        return self.proxy.job_data.jobs[job_num]["status"]

    def error(self, job_num):
        return self.proxy.job_data.jobs[job_num]["error"]


class TheProductsDecide(unittest.TestCase):
    def test_a_finished_job_with_no_products_is_refused_and_retried(self):
        """The 2026-09-18 incident: without this, job 1 is booked as finished and written off."""
        h = PollHarness(
            self,
            responses=[
                {
                    1: state("finished", code=5, error="Error while running CMSSW:"),
                    2: state("finished"),
                }
            ],
            acceptance=0.5,
            tolerance=1.0,
        )
        h.job(1, 0)
        h.job(2, 1)
        h.produce(1)  # only job 2's branch really produced something
        h.run()

        self.assertEqual(h.status(1), h.proxy.job_manager.FAILED)
        self.assertIn("missing outputs", h.error(1))
        self.assertEqual(h.status(2), h.proxy.job_manager.FINISHED)
        # the accepted one is written off by job id, the refused one keeps its id for the retry
        self.assertEqual(
            h.proxy.job_data.jobs[2]["job_id"], h.proxy.job_data.dummy_job_id
        )
        self.assertNotEqual(
            h.proxy.job_data.jobs[1]["job_id"], h.proxy.job_data.dummy_job_id
        )

    def test_a_record_written_since_the_last_listing_is_still_seen(self):
        """The mirror failure, and the reason the check drops its cached listings every poll.

        Both branches are seeds of one point, so they share a directory. Accepting job 1 in the
        first poll caches that directory's listing; job 2's product lands afterwards. Answering
        job 2 from that listing would demote a job that did the work -- one of its retries, a wait
        at the wave gate, and a retry whose own check reads the same stale entry.
        """
        h = PollHarness(
            self,
            responses=[
                {1: state("finished"), 2: state("running")},
                {2: state("finished")},
            ],
            acceptance=1.0,
            tolerance=1.0,
        )
        h.job(1, 0)
        h.job(2, 1)
        h.produce(0)
        # job 2's product lands between the two polls: the first poll's listing, cached while
        # accepting job 1, does not carry it
        h.before_query[1] = lambda: h.produce(1)
        h.run()

        self.assertEqual(
            h.status(2),
            h.proxy.job_manager.FINISHED,
            f"a job that produced its output was demoted: {h.error(2)}",
        )
        self.assertEqual(h.status(1), h.proxy.job_manager.FINISHED)
        # and the price of seeing it: both branches live in one directory, so two polls that
        # consult it cost two listings -- not one per job, which is what confirming every
        # negative with a ~0.9 s stat would have cost
        self.assertEqual(
            h.storage.listings, 2, "the check is one listing per poll per directory"
        )

    def test_a_finished_job_is_checked_once_for_the_whole_run(self):
        """What the check must NOT become: a storage round trip per job per poll.

        Job 1 finishes in the first poll and job 2 only in the third, so the loop runs three
        iterations with job 1 long accepted. law keeps accepted jobs in `finished_jobs` and skips
        them at the top of every later iteration, so its completeness is asked for exactly once.
        """
        h = PollHarness(
            self,
            responses=[
                {1: state("finished"), 2: state("running")},
                {2: state("running")},
                {2: state("finished")},
            ],
            acceptance=1.0,
            tolerance=1.0,
        )
        h.job(1, 0)
        h.job(2, 1)
        h.produce(0)
        h.produce(1)
        h.run()

        self.assertEqual(h.queries, 3, "the loop must really have polled three times")
        self.assertEqual(h.status(1), h.proxy.job_manager.FINISHED)
        self.assertEqual(h.status(2), h.proxy.job_manager.FINISHED)
        self.assertEqual(
            h.complete_calls.get(0),
            1,
            f"branch 0 was re-checked on every poll: {h.complete_calls}",
        )
        self.assertEqual(h.complete_calls.get(1), 1)


class TheSwitchItself(unittest.TestCase):
    def test_crab_workflows_check_completeness(self):
        with mock.patch.object(CrabWorkflow, "_crab_cfg", return_value={}):
            task = RunProd(
                setup=SETUP,
                eras=("Run3_2023BPix",),
                points=("*_M-250",),
                workflow="crab",
            )
        self.assertTrue(
            task.crab_check_job_completeness(),
            "law then accepts CRAB's FINISHED without looking at fs_default",
        )


if __name__ == "__main__":
    unittest.main()
