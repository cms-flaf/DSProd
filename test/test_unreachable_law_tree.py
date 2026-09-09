#!/usr/bin/env python3
"""What a submission round does when law's own tree briefly is not there.

DSProd's software is installed under `soft/` in the checkout that drives the production, so a
submission can hit a moment when law's installed tree cannot be read. What makes that expensive is
not law's bookkeeping but the absence of a handler: the job file is built inside law's `submit()`,
and the `RuntimeError` raised there is caught nowhere between it and luigi, so it propagates out of
`poll()` and the whole workflow is marked FAILED — a 16000-branch production ended twice by one
unreadable path (2026-09-08 and again 2026-09-09). The jobs popped out of `unsubmitted_jobs` a few
statements earlier are not the cost: the only dump between that pop and the job file is the one on
law's nothing-to-submit early return, which is why a restart resumed both times.

The round therefore has to be abandoned *before* law is handed control, which is what these tests
pin: nothing submitted, nothing moved, and the next poll — minutes away — tries again.
"""

import os
import sys
import unittest
from collections import OrderedDict
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod.crab import (  # noqa: E402
    DSProdCrabJobFileFactory,
    DSProdCrabWorkflowProxy,
)
from dsprod.tools import StopOnMassInitialRetryProxy  # noqa: E402

#: what the stand-in for law's own submit returns, so that reaching it is visible in the result
LAW_REACHED = "reached-law"

#: stands in for the errno the storage answers with, so a message assertion cannot depend on it
STORAGE_SAID = "[Errno 13] Permission denied"


def run_submit(missing):
    """Call the real `submit` with the tree probe answering `missing`, on a recording proxy."""
    # a real instance, never __init__'d: constructing a law proxy wants a task and a scheduler,
    # but the `super()` call inside submit() needs the real class, so the collaborators are
    # replaced one by one instead
    proxy = DSProdCrabWorkflowProxy.__new__(DSProdCrabWorkflowProxy)
    proxy.task = mock.Mock()
    proxy.job_data = mock.Mock()
    proxy.job_data.unsubmitted_jobs = OrderedDict({"1": [0], "2": [1]})
    proxy.job_data.jobs = {}
    proxy.stop_on_mass_initial_retry = mock.Mock()
    proxy._update_retry_release_clock = mock.Mock()
    proxy._should_submit_crab_group = mock.Mock(return_value=True)

    # `super(DSProdCrabWorkflowProxy, self).submit` resolves here, and law's real one would try to
    # talk to CRAB. A plain function rather than a mock, so that it records the call whether or not
    # the attribute is looked up as a descriptor -- and it does what law's own submit() does to the
    # backlog first, so that a guard which fails to fire is visible as a moved job and not only as
    # a missing call.
    law_calls = []

    def law_submit(*args, **kwargs):
        law_calls.append((args, kwargs))
        for job_num in list(proxy.job_data.unsubmitted_jobs):
            branches = proxy.job_data.unsubmitted_jobs.pop(job_num)
            proxy.job_data.jobs[job_num] = {"branches": branches}
        return LAW_REACHED

    # the errno is stubbed as well, so that the message assertions do not depend on what the
    # machine running the tests answers for a path under /eos
    with mock.patch.object(
        DSProdCrabJobFileFactory, "missing_law_source", return_value=missing
    ), mock.patch.object(
        DSProdCrabJobFileFactory, "law_source_error", return_value=STORAGE_SAID
    ), mock.patch.object(
        StopOnMassInitialRetryProxy, "submit", new=law_submit
    ):
        result = DSProdCrabWorkflowProxy.submit(proxy)
    return proxy, result, law_calls


class WhenLawsTreeIsUnreachable(unittest.TestCase):
    PATH = "/eos/.../law/contrib/cms/crab/crab_wrapper.sh"

    def test_the_round_is_abandoned_before_law_is_handed_control(self):
        """`stop_on_mass_initial_retry` is the first thing after the guard, so its not being
        called is what proves law never got as far as moving any job."""
        proxy, result, law_calls = run_submit(self.PATH)
        proxy.stop_on_mass_initial_retry.assert_not_called()
        self.assertEqual(law_calls, [])
        self.assertEqual(result, OrderedDict())

    def test_no_job_is_moved_out_of_the_backlog(self):
        proxy, _, _ = run_submit(self.PATH)
        self.assertEqual(list(proxy.job_data.unsubmitted_jobs), ["1", "2"])
        self.assertEqual(proxy.job_data.jobs, {})

    def test_the_reason_is_published_with_the_path_and_what_the_storage_said(self):
        """A verdict a human has to act on: which path, what the storage answered, and that the
        round -- not the production -- is what was given up."""
        proxy, _, _ = run_submit(self.PATH)
        msg = proxy.task.publish_message.call_args.args[0]
        self.assertIn(self.PATH, msg)
        self.assertIn(STORAGE_SAID, msg)
        self.assertIn("nothing is lost", msg)
        self.assertIn("next poll", msg)

    def test_a_readable_tree_does_not_skip_the_round(self):
        """The guard must be invisible in normal operation."""
        proxy, result, law_calls = run_submit(None)
        proxy.stop_on_mass_initial_retry.assert_called_once()
        self.assertEqual(len(law_calls), 1, "law's own submit was not reached")
        self.assertEqual(
            result, LAW_REACHED, "the round was skipped with a readable tree"
        )


class TheProbeItself(unittest.TestCase):
    def test_it_reports_none_when_every_source_is_there(self):
        self.assertIsNone(DSProdCrabJobFileFactory.missing_law_source(retries=0))

    def test_it_names_the_first_unreadable_source(self):
        cls = type(
            "Probe", (DSProdCrabJobFileFactory,), {"law_sources": ("crab/absent.sh",)}
        )
        missing = cls.missing_law_source(retries=0)
        self.assertIsNotNone(missing)
        self.assertTrue(missing.endswith("crab/absent.sh"), missing)

    def test_the_last_resort_guard_still_raises(self):
        """`create()` must not build a job file against a tree that vanished mid-submission."""
        cls = type(
            "Probe",
            (DSProdCrabJobFileFactory,),
            {"law_sources": ("crab/absent.sh",), "source_retries": 0},
        )
        with self.assertRaises(RuntimeError) as caught:
            cls._wait_for_law_sources()
        self.assertIn("unreachable", str(caught.exception))

    def test_it_reports_the_errno_a_missing_path_answers_with(self):
        """`os.path.isfile` hides why it said no, and the why decides who has to act."""
        reason = DSProdCrabJobFileFactory.law_source_error(
            os.path.join(dsprod_repo, "no", "such", "wrapper.sh")
        )
        self.assertIn("[Errno 2]", reason)

    def test_it_separates_a_directory_from_a_storage_error(self):
        """A path that stats perfectly well is not a storage problem, and must not read as one."""
        reason = DSProdCrabJobFileFactory.law_source_error(dsprod_repo)
        self.assertNotIn("Errno", reason)
        self.assertIn("not a regular file", reason)

    def test_it_does_not_sleep_when_asked_not_to(self):
        """It runs inside the poll loop, so the probe must not wait the tree out there."""
        cls = type(
            "Probe", (DSProdCrabJobFileFactory,), {"law_sources": ("crab/absent.sh",)}
        )
        with mock.patch("dsprod.crab.time.sleep") as slept:
            cls.missing_law_source(retries=0)
        slept.assert_not_called()


if __name__ == "__main__":
    unittest.main()
