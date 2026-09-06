#!/usr/bin/env python3
"""One command, two concurrent stages, and merges that fire per group.

The stall this replaces: `NanoMergeTask` required `RunProd`, `workflow_requires()` IS the proxy's
`requires()`, so luigi would not start the merge until every seed of the selection was done --
169 of the 192 Run3_2023BPix groups sat complete and unmerged.

Neither shape law offers can express "this group's own 50 seeds" as a luigi edge, and the tests
below pin why: a per-seed `RunProd.req(branch=n)` is a *branch* task, which luigi runs in-process
(the 7 h chain on the submitting machine), and a per-group narrowed workflow would give every
group its own submission. So the edge is gone and readiness is asked of storage.
"""

import os
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)
os.environ["ANALYSIS_PATH"] = dsprod_repo

import luigi  # noqa: E402
import luigi.interface  # noqa: E402

from dsprod import merge_state  # noqa: E402
from dsprod.tasks import (  # noqa: E402
    MergeWorkflowProxy,
    NanoMergeTask,
    Produce,
    RunProd,
)

SETUP = "models/X_HH/setups/Run3_XHHbbWW.yaml"
ERA = "Run3_2023BPix"


def group(branch, state, **kw):
    kw.setdefault("n_seeds", 50)
    return merge_state.Group(
        branch=branch,
        era=ERA,
        point="P",
        version="v12",
        group=branch,
        state=state,
        n_missing=kw.pop("n_missing", 0),
        n_gone=kw.pop("n_gone", 0),
        **kw
    )


class TestTheGenerationStageIsSubmittedWhole(unittest.TestCase):
    """The scalability property: one remote workflow, not one per merge group.

    A merge group depending on its seeds through law would mean either 192 narrowed `RunProd`
    workflows for one era -- 192 CRAB submissions -- or 9600 branch tasks luigi runs locally.
    `Produce` requires the generation stage once, unnarrowed.
    """

    def setUp(self):
        self._core = luigi.interface.core
        luigi.interface.core = lambda: mock.Mock(workers=2)
        self.addCleanup(lambda: setattr(luigi.interface, "core", self._core))

    def test_one_runprod_workflow_carries_every_branch(self):
        reqs = Produce(setup=SETUP, eras=(ERA,), workflow="crab").requires()
        gen = reqs["generate"]
        self.assertTrue(
            gen.is_workflow(), "a branch task would run in-process, not on CRAB"
        )
        self.assertEqual(gen.branches, (), "the whole era must be one submission set")
        self.assertEqual(len(gen.get_branch_map()), 4800)
        self.assertEqual(gen.workflow, "crab")

    def test_the_merge_runs_locally_whatever_the_generation_backend(self):
        reqs = Produce(setup=SETUP, eras=(ERA,), workflow="crab").requires()
        self.assertEqual(reqs["merge"].workflow, "local")

    def test_the_merge_holds_no_edge_to_the_generation_stage(self):
        merge = NanoMergeTask(setup=SETUP, eras=(ERA,))
        self.assertNotIn("runprod", merge.workflow_requires())
        self.assertEqual(merge.as_branch(0).requires(), {})

    def test_a_single_worker_is_refused(self):
        luigi.interface.core = lambda: mock.Mock(workers=1)
        with self.assertRaises(RuntimeError) as caught:
            Produce(setup=SETUP, eras=(ERA,)).requires()
        self.assertIn("two luigi workers", str(caught.exception))


class TestTheMergeIsLocalByDefault(unittest.TestCase):
    """`find_workflow_cls` walks the MRO, so the proxy has to hang off a workflow class."""

    def test_the_proxy_and_the_default_come_from_the_first_base(self):
        merge = NanoMergeTask(setup=SETUP, eras=(ERA,))
        self.assertEqual(merge.workflow, "local")
        self.assertIs(type(merge.workflow_proxy), MergeWorkflowProxy)

    def test_the_generation_stage_keeps_its_own_default(self):
        self.assertEqual(RunProd(setup=SETUP, eras=(ERA,)).workflow, "htcondor")


class TestTheProxyYieldsOnlyReadyGroups(unittest.TestCase):
    """Each round re-reads storage and yields what can run now."""

    def proxy(self):
        return NanoMergeTask(setup=SETUP, eras=(ERA,)).workflow_proxy

    def test_only_ready_branches_are_yielded(self):
        groups = [
            group(0, merge_state.READY),
            group(1, merge_state.BLOCKED, n_missing=7),
            group(2, merge_state.MERGED),
            group(3, merge_state.READY),
        ]
        with mock.patch.object(merge_state, "classify", return_value=groups):
            req = next(self.proxy().run())
        self.assertIsInstance(req, luigi.DynamicRequirements)
        self.assertEqual(sorted(t.branch for t in req.flat_requirements), [0, 3])

    def test_it_returns_when_every_group_is_merged(self):
        groups = [group(0, merge_state.MERGED), group(1, merge_state.MERGED)]
        with mock.patch.object(merge_state, "classify", return_value=groups):
            with self.assertRaises(StopIteration):
                next(self.proxy().run())

    def test_it_waits_rather_than_yielding_a_blocked_group(self):
        groups = [group(0, merge_state.BLOCKED, n_missing=50)]
        calls = []
        with mock.patch.object(merge_state, "classify", return_value=groups):
            with mock.patch(
                "dsprod.tasks.time.sleep", side_effect=lambda s: calls.append(s)
            ):
                gen = self.proxy().run()
                # first round waits; stop it once it has proved it does not yield a blocked group
                with mock.patch.object(
                    merge_state,
                    "classify",
                    side_effect=[groups, [group(0, merge_state.MERGED)]],
                ):
                    with self.assertRaises(StopIteration):
                        next(gen)
        self.assertTrue(
            calls, "a round with nothing ready must wait, not spin or return"
        )

    def test_groups_whose_inputs_are_gone_stop_the_run_with_the_remedy(self):
        groups = [group(0, merge_state.BROKEN, n_gone=50)]
        with mock.patch.object(merge_state, "classify", return_value=groups):
            with self.assertRaises(RuntimeError) as caught:
                next(self.proxy().run())
        message = str(caught.exception)
        self.assertIn("cannot be merged", message)
        self.assertIn("produced/", message)


if __name__ == "__main__":
    unittest.main()
