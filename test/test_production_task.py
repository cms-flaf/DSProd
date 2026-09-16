#!/usr/bin/env python3
"""One command for a whole production: produce, repair what storage lost, merge.

These do not mock the orchestration -- they run it. `ProductionTask.run()` is a generator of dynamic
dependencies, and the thing worth pinning is what luigi does with it: that a repair between the two
stages really makes `RunProd` run a second time, that the sequence terminates, and that a finished
production is a no-op. Only `RunProd`, `NanoMergeTask` and `PruneProducedRecords` are replaced, by
stand-ins whose completeness is a dict this file owns; luigi's scheduler and worker are real.

The order matters and is forced by luigi: a task whose dynamic dependency FAILS is never re-run, so
the repair cannot be a reaction to a failed merge -- it has to run before one.
"""

import contextlib
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

import law  # noqa: E402
import luigi  # noqa: E402

law.contrib.load("cms")

import dsprod.tasks as tasks  # noqa: E402
from dsprod.tasks import ProductionTask, PruneProducedRecords  # noqa: E402

SETUP = "models/X_HH/setups/Run3_XHHbbWW.yaml"

#: what the stand-ins read and write, and the order in which they ran
state = {}


class Stub(law.Task):
    """A stand-in for one of the real stages, complete when `state` says so."""

    setup = luigi.Parameter()
    workflow = luigi.Parameter(default="local")

    key = None

    def complete(self):
        return state[self.key]

    def run(self):
        state["order"].append(self.key)
        state[self.key] = True

    def output(self):
        return []


class StubRunProd(Stub):
    key = "produced"


class StubNanoMergeTask(Stub):
    key = "merged"


class StubPrune(law.Task):  # noqa: E302
    """The repair. Deleting a record makes its seed unproduced again -- as on storage."""

    setup = luigi.Parameter()
    prune = luigi.BoolParameter(default=False)

    n_pruned = 0

    def run(self):
        state["order"].append("repair")
        self.n_pruned = state["stale"]
        if state["stale"]:
            state["stale"] = 0
            state["produced"] = False


def production(**kwargs):
    task = ProductionTask(setup=SETUP, points=("*_M-250",), **kwargs)
    # luigi caches task instances by their parameters, so the repair count of a previous test
    # would otherwise carry into this one -- within a real run that persistence is the point
    task._rounds = 0
    return task


def drive(stale=0, produced=False, merged=False, **kwargs):
    """Run a production through luigi and return the order the stages ran in."""
    state.clear()
    state.update(order=[], stale=stale, produced=produced, merged=merged, failures=[])
    with mock.patch("dsprod.tasks.RunProd", StubRunProd), mock.patch(
        "dsprod.tasks.NanoMergeTask", StubNanoMergeTask
    ), mock.patch("dsprod.tasks.PruneProducedRecords", StubPrune):
        ok = luigi.build(
            [production(**kwargs)],
            local_scheduler=True,
            workers=1,
            log_level="CRITICAL",
        )
    return ok, list(state["order"])


@ProductionTask.event_handler(luigi.Event.FAILURE)
def _record_failure(task, exception):
    """So a test can assert *why* a run failed, not only that it did."""
    state.setdefault("failures", []).append(str(exception))


class ItRunsTheWholeChain(unittest.TestCase):
    def setUp(self):
        if not os.path.exists(os.path.join(dsprod_repo, SETUP)):
            self.skipTest("models submodule not checked out")

    def test_produce_then_repair_then_merge(self):
        ok, order = drive()
        self.assertTrue(ok)
        self.assertEqual(order, ["produced", "repair", "merged"])

    def test_a_repair_sends_the_pruned_seeds_back_through_production(self):
        """The point of the whole task: a record whose file was lost becomes a seed produced
        again, instead of a merge group that fails on every attempt."""
        ok, order = drive(stale=3)
        self.assertTrue(ok)
        self.assertEqual(order, ["produced", "repair", "produced", "repair", "merged"])

    def test_it_terminates_rather_than_looping(self):
        """What #47 was: a never-complete dependency is rescheduled on every pass for ever."""
        ok, order = drive(stale=2)
        self.assertTrue(ok)
        self.assertEqual(order.count("merged"), 1)
        self.assertLessEqual(len(order), 6)

    def test_a_finished_production_is_a_no_op(self):
        ok, order = drive(produced=True, merged=True)
        self.assertTrue(ok)
        self.assertEqual(order, [])

    def test_an_interrupted_production_resumes_at_the_merge(self):
        ok, order = drive(produced=True)
        self.assertTrue(ok)
        self.assertEqual(order, ["repair", "merged"])

    def test_the_repair_can_be_switched_off(self):
        ok, order = drive(stale=3, max_repairs=0)
        self.assertTrue(ok)
        self.assertEqual(order, ["produced", "merged"])


@contextlib.contextmanager
def local_storage(records, staged, merged=()):
    """A real `fs_default` on local disk, holding one point of one era."""
    directory = tempfile.mkdtemp(prefix="dsprod_prod_task_")
    point = "GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-250"
    # the shipped setup's `output`, which is what the task builds its paths from
    root = os.path.join(directory, "XHHbbWW")
    paths = {
        "produced": os.path.join(root, "produced/nanoAOD_v12/Run3_2022EE", point),
        "staging": os.path.join(root, "staging/nanoAOD_v12/Run3_2022EE", point),
        "merged": os.path.join(root, "nanoAOD_v12/Run3_2022EE", point),
    }
    for path in paths.values():
        os.makedirs(path)
    for seed in records:
        with open(os.path.join(paths["produced"], f"nano_v12_{seed}.json"), "w") as f:
            f.write('{"events_requested": 1000}')
    for seed in staged:
        open(os.path.join(paths["staging"], f"nano_v12_{seed}.root"), "w").close()
    for group in merged:
        open(os.path.join(paths["merged"], f"nano_v12_{group}.root"), "w").close()
    cached = dict(tasks._fs_cache)
    try:
        tasks._fs_cache.clear()
        tasks._fs_cache["fs_default"] = law.LocalFileSystem(base=directory)
        yield paths
    finally:
        tasks._fs_cache.clear()
        tasks._fs_cache.update(cached)
        shutil.rmtree(directory, ignore_errors=True)


class StorageBackedRunProd(Stub):
    """`RunProd` as far as this matters: complete when every seed has its `produced/` record, and
    a run writes back the ones that are missing. That is the real coupling -- the pruner deletes a
    record, so production has something to do again -- and a stub with its own idea of
    completeness cannot exercise it."""

    key = "produced"

    def complete(self):
        return not self._missing()

    #: a production that writes the record but not the nano file -- i.e. storage still losing it
    lossy = False

    def run(self):
        state["order"].append("produced")
        # a bound that stopped working would otherwise leave these tests looping until something
        # kills them; CI installs law and luigi unpinned, so that is a real possibility
        if len(state["order"]) > state["max_runs"]:
            raise AssertionError(
                f"production ran {len(state['order'])} times: the repair bound is not holding"
            )
        for seed in self._missing():
            if not self.lossy:
                open(
                    os.path.join(state["staging_dir"], f"nano_v12_{seed}.root"), "w"
                ).close()
            with open(
                os.path.join(state["produced_dir"], f"nano_v12_{seed}.json"), "w"
            ) as f:
                f.write('{"events_requested": 1000}')

    def _missing(self):
        have = set(os.listdir(state["produced_dir"]))
        return [s for s in state["seeds"] if f"nano_v12_{s}.json" not in have]


class TheRepairIsTheRealOne(unittest.TestCase):
    """With the real `PruneProducedRecords` against real storage -- only the two batch stages are
    stood in for. Nothing else connects the pruner's delete mode and its count to the driver.
    """

    def setUp(self):
        if not os.path.exists(os.path.join(dsprod_repo, SETUP)):
            self.skipTest("models submodule not checked out")

    def drive_for_real(self, paths, seeds, **kwargs):
        state.clear()
        state.update(
            order=[],
            stale=0,
            merged=False,
            seeds=list(seeds),
            produced_dir=paths["produced"],
            staging_dir=paths["staging"],
            max_runs=int(kwargs.get("max_repairs", 2)) + 2,
            failures=[],
        )
        with mock.patch("dsprod.tasks.RunProd", StorageBackedRunProd), mock.patch(
            "dsprod.tasks.NanoMergeTask", StubNanoMergeTask
        ):
            ok = luigi.build(
                [production(**kwargs)],
                local_scheduler=True,
                workers=1,
                log_level="CRITICAL",
            )
        return ok, list(state["order"])

    def test_a_lost_file_costs_its_record_and_the_seed_is_produced_again(self):
        """The incident of 2026-09-15, driven end to end: 50 records, 49 staged files, no merged
        file. The record of the missing one must be deleted from disk, and production must run a
        second time for it."""
        seeds = range(1, 51)
        with local_storage(
            records=seeds, staged=[s for s in seeds if s != 45]
        ) as paths:
            record = os.path.join(paths["produced"], "nano_v12_45.json")
            self.assertTrue(os.path.exists(record))
            ok, order = self.drive_for_real(paths, seeds)
            self.assertTrue(ok)
            # deleted by the real pruner, then written back by production -- which is the point
            self.assertEqual(len(os.listdir(paths["produced"])), 50)
            self.assertTrue(os.path.exists(record))
        # the real pruner keeps no bookkeeping of its own here: what says it ran is the record
        # that is gone, and the production stage running again for the seed behind it
        self.assertEqual(order, ["produced", "merged"])

    def test_a_healthy_production_loses_no_record_and_goes_straight_to_the_merge(self):
        seeds = range(1, 51)
        with local_storage(records=seeds, staged=seeds) as paths:
            ok, order = self.drive_for_real(paths, seeds)
            self.assertTrue(ok)
            self.assertEqual(len(os.listdir(paths["produced"])), 50)
        # nothing was pruned, so nothing was produced again
        self.assertEqual(order, ["merged"])

    def test_a_storage_that_keeps_losing_the_file_stops_the_run(self):
        """The bound `max_repairs` is there for: each round produces the seed again, and if the
        file keeps disappearing the run must stop rather than keep submitting."""
        seeds = range(1, 51)
        with local_storage(
            records=seeds, staged=[s for s in seeds if s != 45]
        ) as paths:
            with mock.patch.object(StorageBackedRunProd, "lossy", True):
                ok, order = self.drive_for_real(paths, seeds, max_repairs=2)
        self.assertFalse(
            ok, "a production that cannot be repaired must not report success"
        )
        self.assertEqual(
            order.count("produced"), 2, "one production per allowed repair round"
        )
        # for the documented reason, not merely somehow
        self.assertTrue(
            any("repair round" in f for f in state["failures"]),
            f"stopped for the wrong reason: {state['failures']}",
        )
        self.assertNotIn("merged", order)

    def test_the_driver_asks_for_deletion_not_for_a_report(self):
        """`--prune` defaults to off on the standalone task, on purpose. The driver must override
        it, or every sweep reports and deletes nothing while the merge is submitted regardless.
        """
        pruner = PruneProducedRecords.req(ProductionTask(setup=SETUP), prune=True)
        self.assertTrue(pruner.prune)
        self.assertEqual(pruner.setup, SETUP)


class AndItIsBuiltTheWayLawAllows(unittest.TestCase):
    def setUp(self):
        if not os.path.exists(os.path.join(dsprod_repo, SETUP)):
            self.skipTest("models submodule not checked out")

    def test_it_is_not_a_workflow(self):
        """A law local workflow re-checks its branches on every pass, which is the loop this task
        would otherwise inherit from `PruneProducedRecords`."""
        self.assertNotIsInstance(production(), law.LocalWorkflow)

    def test_completeness_is_the_merged_files(self):
        state.clear()
        state.update(order=[], stale=0, produced=True, merged=False)
        with mock.patch("dsprod.tasks.NanoMergeTask", StubNanoMergeTask):
            self.assertFalse(production().complete())
            state["merged"] = True
            self.assertTrue(production().complete())

    def test_more_than_one_worker_is_refused_while_a_repair_is_allowed(self):
        """The repair bound is kept in memory, and luigi runs a task in its own process with
        several workers -- so the configuration is refused rather than left to degrade.
        """
        from luigi.cmdline_parser import CmdlineParser

        with CmdlineParser.global_instance(
            ["ProductionTask", "--setup", SETUP, "--workers", "4"]
        ):
            with self.assertRaises(RuntimeError) as caught:
                production()._require_one_process()
        self.assertIn("--max-repairs 0", str(caught.exception))

    def test_a_timeout_set_on_the_task_counts_too(self):
        """luigi forces multiprocessing for a task carrying its own timeout, not only for the
        global one."""
        with mock.patch.object(ProductionTask, "worker_timeout", 60):
            with self.assertRaises(RuntimeError):
                production()._require_one_process()

    def test_a_worker_timeout_is_refused_for_the_same_reason(self):
        from luigi.cmdline_parser import CmdlineParser

        with CmdlineParser.global_instance(
            ["ProductionTask", "--setup", SETUP, "--worker-timeout", "60"]
        ):
            with self.assertRaises(RuntimeError):
                production()._require_one_process()

    def test_but_workers_are_allowed_with_the_repair_switched_off(self):
        """`--backend local` runs its branches in parallel through `--workers`, which is what the
        rest of the documentation tells an operator to use; with no repair there is no count to
        lose."""
        from luigi.cmdline_parser import CmdlineParser

        with CmdlineParser.global_instance(
            ["ProductionTask", "--setup", SETUP, "--workers", "8"]
        ):
            production(max_repairs=0)._require_one_process()

    def test_and_a_plain_single_worker_run_is_silent(self):
        production()._require_one_process()

    def test_the_guard_runs_before_anything_else(self):
        """Removing the call would leave every other test green."""
        from luigi.cmdline_parser import CmdlineParser

        with CmdlineParser.global_instance(
            ["ProductionTask", "--setup", SETUP, "--workers", "4"]
        ):
            with mock.patch("dsprod.tasks.RunProd", StubRunProd), mock.patch(
                "dsprod.tasks.NanoMergeTask", StubNanoMergeTask
            ), mock.patch("dsprod.tasks.PruneProducedRecords", StubPrune):
                state.clear()
                state.update(
                    order=[], stale=0, produced=False, merged=False, failures=[]
                )
                with self.assertRaises(RuntimeError):
                    list(production().run())
        self.assertEqual(
            state["order"], [], "nothing may be submitted before the refusal"
        )

    def test_every_backend_reaches_both_stages(self):
        """All three, `local` included -- it is what a first run is told to use."""
        with mock.patch("dsprod.tasks.RunProd", StubRunProd), mock.patch(
            "dsprod.tasks.NanoMergeTask", StubNanoMergeTask
        ):
            for backend in ("crab", "htcondor", "local"):
                task = production(backend=backend)
                self.assertEqual(task._produce().workflow, backend)
                self.assertEqual(task._merge().workflow, backend)

    def test_the_selection_reaches_both_stages(self):
        with mock.patch("dsprod.tasks.RunProd", StubRunProd), mock.patch(
            "dsprod.tasks.NanoMergeTask", StubNanoMergeTask
        ):
            task = production()
            self.assertEqual(task._produce().setup, SETUP)
            self.assertEqual(task._merge().setup, SETUP)


if __name__ == "__main__":
    unittest.main()
