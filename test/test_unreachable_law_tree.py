#!/usr/bin/env python3
"""What a submission round does when law's own tree briefly is not there.

DSProd's software lives on EOS and its `soft/` is a symlink into AFS, so a submission can hit a
moment when law's installed tree cannot be read. The order of operations inside law's `submit()` is
what makes that expensive: it pops the jobs it is about to send out of `unsubmitted_jobs`, creates
empty `job_data.jobs` entries for them, and only *then* builds the job file — where the unreadable
tree is discovered. The `RuntimeError` raised there propagates out of `poll()` and luigi marks the
whole workflow FAILED: a 16000-branch production lost to a blip in a mount (2026-09-08 and again
2026-09-09).

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
    # the attribute is looked up as a descriptor.
    law_calls = []

    def law_submit(*args, **kwargs):
        law_calls.append((args, kwargs))
        return LAW_REACHED

    with mock.patch.object(
        DSProdCrabJobFileFactory, "missing_law_source", return_value=missing
    ), mock.patch.object(StopOnMassInitialRetryProxy, "submit", new=law_submit):
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

    def test_the_reason_is_published_with_the_path_and_what_to_check(self):
        proxy, _, _ = run_submit(self.PATH)
        msg = proxy.task.publish_message.call_args.args[0]
        self.assertIn(self.PATH, msg)
        self.assertIn("nothing is lost", msg)
        self.assertIn("aklog", msg)

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
