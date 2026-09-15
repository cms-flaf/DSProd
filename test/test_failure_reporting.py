#!/usr/bin/env python3
"""Why a job failed, in the job's own words.

CRAB's exit code is a label, not a diagnosis: every one of the 4197 failures of one production
carried exit 5, "Error while running CMSSW", and law repeats exactly that. What says what happened
is the exception DSProd's payload raised, and it sits in the job's stdout on the schedd.

On 2026-09-15 three merge jobs of the Run3_2022EE production failed on every resubmission. The
driver reported three job ids and exit 5; the actual message -- "1 of 50 staged nano files of this
merge group are gone (first: ...M-2800/nano_v12_45.root)", which names the file, the seed and the
repair -- was found only by locating the scheduler's web directory by hand. These tests pin the two
halves of not repeating that: reading the payload's error out of a job's stdout, and the merge
message being worth reading when it gets there.
"""

import itertools
import os
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

import law  # noqa: E402

law.contrib.load("cms")

from dsprod.crab import (  # noqa: E402
    DSProdCrabJobManager,
    fetch_job_stdout,
    payload_error,
)
from dsprod.tasks import NanoMergeTask  # noqa: E402

SANDBOX = "bash::/dev/null"

#: the real thing, trimmed: the tail of one of the three job logs of 2026-09-15
REAL_LOG = """\
== CMSSW: Begin processing
/srv/gWMS-CMSRunAnalysis.sh: line 45: ./submit_env.sh: No such file or directory
== CMSSW: Traceback (most recent call last):
== CMSSW:   File "<string>", line 1, in <module>
== CMSSW: ImportError: No module named FWCore.ParameterSet.Config
== CMSSW: branch=65
== CMSSW: Traceback (most recent call last):
== CMSSW:   File "/srv/dsprod/tasks.py", line 1270, in run
== CMSSW:     raise RuntimeError(
== CMSSW: RuntimeError: 1 of 50 staged nano files of this merge group are gone -- seeds 45, e.g. \
root://cmseos.fnal.gov//eos/uscms/store/user/x/nano_v12_45.root -- although their seeds are \
recorded as produced.
Error executing application in CMSSW environment.
"""


def manager(**attrs):
    m = DSProdCrabJobManager(sandbox_name=SANDBOX)
    for k, v in attrs.items():
        setattr(m, k, v)
    return m


def failed(crab_num, log="http://x/job_out.1.0.txt", code=5, site="T2_CH_CERN"):
    job_id = DSProdCrabJobManager.JobId(crab_num, "260915:task", "/proj")
    extra = {"site_history": [site] if site else []}
    if log:
        extra["log_file"] = log
    return job_id, {
        "status": DSProdCrabJobManager.FAILED,
        "code": code,
        "error": "Error while running CMSSW:",
        "extra": extra,
    }


def result(*jobs):
    return dict(jobs)


class ReadingThePayloadsError(unittest.TestCase):
    def test_the_last_exception_is_the_one_that_ended_the_job(self):
        """A job log carries harmless earlier exceptions -- the probe that imports FWCore before
        the release is set up always leaves an ImportError behind."""
        found = payload_error(REAL_LOG)
        self.assertTrue(
            found.startswith("RuntimeError: 1 of 50 staged nano files"), found
        )

    def test_a_log_without_an_exception_says_so_rather_than_inventing_one(self):
        self.assertIsNone(payload_error("all fine\nnothing to see\n"))

    def test_a_very_long_message_is_trimmed(self):
        found = payload_error("RuntimeError: " + "x" * 5000)
        self.assertLess(len(found), 1000)
        self.assertTrue(found.endswith("..."))

    def test_an_exception_with_a_dotted_name_is_recognised(self):
        self.assertEqual(
            payload_error("== CMSSW: law.target.RemoteFileError: gone"),
            "law.target.RemoteFileError: gone",
        )


class FetchingTheLogCannotHoldUpThePoll(unittest.TestCase):
    """`query()` runs in law's thread pool and `get_async_result_silent` waits on it without a
    timeout of its own, so every project of the production waits for whatever this does.
    """

    class Response:
        def __init__(self, status, chunks):
            self.status = status
            self._chunks = list(chunks)

        def read(self, n):
            return self._chunks.pop(0) if self._chunks else b""

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fetch(self, response, **kwargs):
        with mock.patch("os.path.exists", return_value=True), mock.patch(
            "ssl.create_default_context"
        ), mock.patch.dict(os.environ, {"X509_USER_PROXY": "/tmp/proxy"}):
            with mock.patch("urllib.request.urlopen", return_value=response) as opened:
                return fetch_job_stdout("http://x/job_out.1.0.txt", **kwargs), opened

    def test_the_tail_is_asked_for_by_range(self):
        text, opened = self.fetch(self.Response(206, [b"the tail\n"]))
        self.assertEqual(text, "the tail\n")
        request = opened.call_args.args[0]
        self.assertIn("bytes=-", request.headers.get("Range", ""))

    def test_a_server_that_ignores_the_range_is_not_followed_forever(self):
        """The runaway log a tail is for: without a bound this reads gigabytes into a poll."""
        endless = self.Response(200, [b"x" * 4096] * 100000)
        with self.assertRaises(RuntimeError) as caught:
            self.fetch(endless, max_bytes=4096, deadline=60.0)
        self.assertIn("still arriving", str(caught.exception))

    def test_a_ranged_response_that_trickles_is_cut_off_too(self):
        """The bytes of a 206 are already bounded -- the tail is all it sends -- but the time is
        not, and `timeout` bounds one socket read rather than a transfer that dribbles.

        The clock is injected rather than waited on: asking whether real time passed between two
        in-memory reads is a test that passes or fails by luck.
        """
        ticks = itertools.count(0.0, 100.0)
        with mock.patch("dsprod.crab.time.monotonic", side_effect=lambda: next(ticks)):
            with self.assertRaises(RuntimeError) as caught:
                self.fetch(self.Response(206, [b"x" * 4096] * 10), deadline=60.0)
        self.assertIn("still arriving", str(caught.exception))

    def test_a_log_that_fits_is_returned_whole(self):
        text, _ = self.fetch(self.Response(200, [b"short log\n"]), max_bytes=4096)
        self.assertEqual(text, "short log\n")

    def test_without_a_proxy_it_says_so_instead_of_trying(self):
        with mock.patch.dict(os.environ, {"X509_USER_PROXY": ""}):
            with self.assertRaises(RuntimeError) as caught:
                fetch_job_stdout("http://x/job_out.1.0.txt")
        self.assertIn("X509_USER_PROXY", str(caught.exception))


class ReportingAFailedJob(unittest.TestCase):
    def report(self, m, res):
        with mock.patch("dsprod.crab.fetch_job_stdout", return_value=REAL_LOG) as fetch:
            with mock.patch("builtins.print") as printed:
                m.report_failures(res)
        return fetch, [c.args[0] for c in printed.call_args_list]

    def test_the_reason_the_site_and_the_code_are_printed(self):
        m = manager()
        _, lines = self.report(m, result(failed(66)))
        self.assertEqual(len(lines), 1)
        self.assertIn("RuntimeError: 1 of 50 staged nano files", lines[0])
        self.assertIn("T2_CH_CERN", lines[0])
        self.assertIn("exit code 5", lines[0])
        self.assertIn("66", lines[0])

    def test_a_poll_that_repeats_does_not_report_again(self):
        """The same failure comes back on every poll until the job is resubmitted."""
        m = manager()
        res = result(failed(66))
        fetch, lines = self.report(m, res)
        self.assertEqual(len(lines), 1)
        self.assertEqual(fetch.call_count, 1)
        fetch2, lines2 = self.report(m, res)
        self.assertEqual(lines2, [])
        self.assertEqual(fetch2.call_count, 0)

    def test_a_new_attempt_is_reported_again(self):
        """The log URL carries the attempt, so the next try's failure is news."""
        m = manager()
        self.report(m, result(failed(66, log="http://x/job_out.1.0.txt")))
        _, lines = self.report(m, result(failed(66, log="http://x/job_out.1.1.txt")))
        self.assertEqual(len(lines), 1)

    def test_law_bookkeeping_is_not_a_payload_failure(self):
        """A failure without a job-level code is a kill or law's own resync -- there is no
        payload stdout to read, and `harvest_site_stats` ignores it for the same reason.
        """
        m = manager()
        fetch, lines = self.report(m, result(failed(66, code=None)))
        self.assertEqual(lines, [])
        self.assertEqual(fetch.call_count, 0)

    def test_a_failure_with_no_log_url_is_skipped_rather_than_raising(self):
        """law records no `log_file` until it has parsed a scheduler id out of the status."""
        m = manager()
        fetch, lines = self.report(m, result(failed(66, log=None)))
        self.assertEqual(lines, [])
        self.assertEqual(fetch.call_count, 0)

    def test_an_entry_that_is_not_a_status_dict_is_skipped(self):
        """law's query data is not guaranteed to carry only dicts, and this runs in the poll."""
        m = manager()
        job_id = DSProdCrabJobManager.JobId(66, "260915:task", "/proj")
        with mock.patch("dsprod.crab.fetch_job_stdout", return_value=REAL_LOG):
            with mock.patch("builtins.print") as printed:
                m.report_failures({job_id: None, "x": "not a dict"})
        self.assertEqual(printed.call_args_list, [])

    def test_a_finished_job_is_not_reported(self):
        m = manager()
        job_id, data = failed(66)
        data["status"] = DSProdCrabJobManager.FINISHED
        fetch, lines = self.report(m, {job_id: data})
        self.assertEqual(lines, [])
        self.assertEqual(fetch.call_count, 0)

    def test_a_wave_of_failures_is_capped_and_says_that_it_capped(self):
        """A production that fails by the hundred fails for a handful of reasons, and the poll
        must not wait on one HTTP fetch per job."""
        m = manager(max_failure_reports=2)
        jobs = [failed(n, log=f"http://x/job_out.{n}.0.txt") for n in range(1, 8)]
        fetch, lines = self.report(m, result(*jobs))
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(len(lines), 3)
        self.assertIn("5 more failed job(s)", lines[-1])
        self.assertIn("max_failure_reports=2", lines[-1])

    def test_a_log_that_cannot_be_read_is_said_out_loud(self):
        """The one thing a diagnostic must not do is fail quietly."""
        m = manager()
        with mock.patch(
            "dsprod.crab.fetch_job_stdout", side_effect=RuntimeError("401 unauthorized")
        ):
            with mock.patch("builtins.print") as printed:
                m.report_failures(result(failed(66)))
        line = printed.call_args_list[0].args[0]
        self.assertIn("could not read its stdout", line)
        self.assertIn("401 unauthorized", line)
        self.assertIn("http://x/job_out.1.0.txt", line)

    def test_a_log_without_an_exception_is_also_said_out_loud(self):
        m = manager()
        with mock.patch("dsprod.crab.fetch_job_stdout", return_value="nothing here"):
            with mock.patch("builtins.print") as printed:
                m.report_failures(result(failed(66)))
        self.assertIn("carries no exception", printed.call_args_list[0].args[0])

    def test_reporting_never_raises_into_the_poll(self):
        """It runs inside `query()`: an exception here would cost the whole poll -- every other
        project's status with it -- to print a message."""
        m = manager()
        with mock.patch("dsprod.crab.fetch_job_stdout", side_effect=KeyboardInterrupt):
            with mock.patch("builtins.print"):
                with self.assertRaises(KeyboardInterrupt):
                    m.report_failures(result(failed(66)))
        # anything short of a BaseException is swallowed into the message
        for boom in (OSError("down"), ValueError("weird"), Exception("x")):
            with mock.patch("dsprod.crab.fetch_job_stdout", side_effect=boom):
                with mock.patch("builtins.print"):
                    m.report_failures(result(failed(70, log=f"http://x/{boom}.txt")))


class TheMergeMessageIsWorthReading(unittest.TestCase):
    """What the fetched log should contain when the merge is what failed."""

    def check(self, missing_seeds, n_seeds=50):
        seeds = list(range(1, n_seeds + 1))

        def staged(era, point, version, seed):
            target = mock.Mock()
            target.exists.return_value = seed not in missing_seeds
            target.uri.return_value = f"root://x//staging/nano_{version}_{seed}.root"
            return target

        def produced(era, point, version, seed):
            target = mock.Mock()
            target.uri.return_value = f"root://x//produced/nano_{version}_{seed}.json"
            return target

        task = mock.Mock()
        task.staged_nano_target = staged
        task.produced_nano_target = produced
        with self.assertRaises(RuntimeError) as caught:
            NanoMergeTask._check_staged_inputs(task, "Run3_2022EE", "P", "v12", seeds)
        return str(caught.exception)

    def test_every_missing_seed_is_named(self):
        message = self.check({7, 23, 45})
        self.assertIn("3 of 50", message)
        for seed in (7, 23, 45):
            self.assertIn(str(seed), message)

    def test_the_record_to_delete_is_named(self):
        """The repair is deleting `produced/` records, so the message hands over one path."""
        message = self.check({45})
        self.assertIn("produced/nano_v12_45.json", message)
        self.assertIn("PruneProducedRecords", message)

    def test_a_long_list_is_cut_with_a_count(self):
        message = self.check(set(range(1, 41)))
        self.assertIn("40 of 50", message)
        self.assertIn("and 28 more", message)

    def test_nothing_missing_returns_the_targets(self):
        seeds = [1, 2, 3]
        task = mock.Mock()
        task.staged_nano_target = lambda e, p, v, s: mock.Mock(
            **{"exists.return_value": True}
        )
        staged = NanoMergeTask._check_staged_inputs(task, "e", "p", "v12", seeds)
        self.assertEqual(len(staged), 3)


if __name__ == "__main__":
    unittest.main()
