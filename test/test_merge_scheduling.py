#!/usr/bin/env python3
"""The merge waited for the whole generation stage, and nothing said which groups could run.

In the Run3_2023BPix production (4800 `RunProd` branches, 192 merge groups) 169 groups were
complete and none had merged, because `NanoMergeTask.workflow_requires()` required the entire
`RunProd` workflow. Narrowing it to the seeds of the groups actually being merged is what these
tests pin down -- including the numbering, since law copies `branches` through `req()` and
`--branches 5` on the merge therefore used to ask for *RunProd* branch 5 rather than for the 50
seeds of merge group 5.
"""

import os
import shutil
import sys
import tempfile
import unittest
from argparse import Namespace
from io import StringIO
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for path in (dsprod_repo, os.path.join(dsprod_repo, "run_tools")):
    if path not in sys.path:
        sys.path.insert(0, path)

# `Task.to_abs` resolves the setup path against $ANALYSIS_PATH, and the checkout is the area
os.environ["ANALYSIS_PATH"] = dsprod_repo

import law  # noqa: E402
from luigi.cmdline_parser import CmdlineParser  # noqa: E402

import merge_status  # noqa: E402
from dsprod.tasks import NanoMergeTask, RunProd  # noqa: E402

SETUP = "models/X_HH/setups/Run3_XHHbbWW.yaml"
ERA = "Run3_2023BPix"

#: the production the numbers here come from: 4800 seeds, 50 per merge, 192 groups over v12 + v15
N_RUNPROD = 4800
N_GROUPS = 192
FILES_PER_MERGE = 50

#: the first two points of the setup in this era, 150 seeds each (150 000 events at 1000 per job)
FIRST_POINT = "GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-250"
SECOND_POINT = "GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-260"
SEEDS_PER_POINT = 150

_tmp = None
_fs_patcher = None


def setUpModule():
    """Point `fs_default` at a local directory, so no test needs a VOMS proxy or the endpoint."""
    global _tmp, _fs_patcher
    _tmp = tempfile.mkdtemp(prefix="dsprod_merge_test_")
    os.environ["ANALYSIS_DATA_PATH"] = os.path.join(_tmp, "data")
    _fs_patcher = mock.patch(
        "dsprod.tasks.get_fs",
        return_value=law.LocalFileSystem(base=os.path.join(_tmp, "store")),
    )
    _fs_patcher.start()


def tearDownModule():
    _fs_patcher.stop()
    shutil.rmtree(_tmp, ignore_errors=True)


def merge_task(**kwargs):
    """The merge workflow over one full era of the real setup."""
    kwargs.setdefault("workflow", "htcondor")
    return NanoMergeTask(setup=SETUP, eras=(ERA,), **kwargs)


class TestRequiredRunProdBranches(unittest.TestCase):
    """Which `RunProd` branches a merge workflow makes its requirement.

    The expected branch ids are written out rather than recomputed: `runprod_branches` numbers
    (era, point, seed) in that order, so the 150 seeds of the setup's first point are 0-149 and
    those of its second are 150-299.
    """

    def test_full_map_requires_every_seed(self):
        # law collapses `branches` back to empty when it selects the whole map, so driving the
        # production through the merge polls the same job data as `law run RunProd` itself --
        # a narrowed `branches` here would mean a second, competing set of job ids.
        task = merge_task()
        self.assertEqual(len(task.get_branch_map()), N_GROUPS)
        self.assertEqual(task.required_runprod_branches(), list(range(N_RUNPROD)))

        required = task.workflow_requires()["runprod"]
        self.assertEqual(len(required.get_branch_map()), N_RUNPROD)
        self.assertEqual(required.branches, ())
        self.assertEqual(required.get_branches_repr(), "0To4800")

    def test_a_whole_selection_keeps_the_job_data_file_name(self):
        # the same claim at the file `drive.sh` and the staleness alarm watch, on a two-point
        # selection so the assertion does not cost 4800 branch tasks: `branches` collapses to
        # empty, `control_output_postfix()` is `get_branches_repr()`, and the name is the one
        # `law run RunProd --points '*_M-1200'` writes.
        task = merge_task(points=("*_M-1200",))
        required = task.workflow_requires()["runprod"]
        self.assertEqual(len(required.get_branch_map()), 100)
        self.assertEqual(
            os.path.basename(required.workflow_proxy.get_cached_output()["jobs"].path),
            "htcondor_jobs_0To100.json",
        )
        # ... down to the full path `law run RunProd --points '*_M-1200'` itself would write
        plain = RunProd(
            setup=SETUP, eras=(ERA,), points=("*_M-1200",), workflow="htcondor"
        )
        self.assertEqual(
            required.workflow_proxy.get_cached_output()["jobs"].path,
            plain.workflow_proxy.get_cached_output()["jobs"].path,
        )

    def test_one_group_requires_only_its_own_seeds(self):
        # merge branch 0 = first point, v12, group 0 -> its seeds 1-50 -> RunProd 0-49
        task = merge_task(branches=(0,))
        self.assertEqual(task.required_runprod_branches(), list(range(50)))

        required = task.workflow_requires()["runprod"]
        branch_map = required.get_branch_map()
        self.assertEqual(len(branch_map), FILES_PER_MERGE)
        self.assertEqual(
            sorted(branch_map.values()),
            [(ERA, 0, seed) for seed in range(1, FILES_PER_MERGE + 1)],
        )
        # ... and nothing beyond them: the whole point is that the other 4750 seeds may still
        # be running
        self.assertNotIn(N_RUNPROD - 1, branch_map)

    def test_a_branch_restriction_is_not_a_runprod_restriction(self):
        # what the requirement used to be: law copies `branches` through `req()`, so the merge's
        # own branch ids were handed to RunProd as if they were seeds -- merge group 0 needs 50
        # RunProd branches, not the one that shares its number
        task = merge_task(branches=(0,))
        self.assertEqual(len(RunProd.req(task).get_branch_map()), 1)
        self.assertEqual(len(task.workflow_requires()["runprod"].get_branch_map()), 50)

    def test_groups_of_several_points_take_the_union(self):
        # merge branches 2, 6, 7 = first point v12 group 2 (seeds 101-150 -> RunProd 100-149),
        # second point v12 groups 0 and 1 (seeds 1-100 -> RunProd 150-249)
        task = merge_task(branches=(2, (6, 8)))
        self.assertEqual(len(task.get_branch_map()), 3)
        self.assertEqual(task.required_runprod_branches(), list(range(100, 250)))
        self.assertEqual(task.workflow_requires()["runprod"].branches, ((100, 250),))

    def test_both_nano_versions_of_a_group_share_its_seeds(self):
        # one RunProd job stages one file per nano version, so merge branches 0 (v12 group 0) and
        # 3 (v15 group 0) of the first point are owed by the same 50 seeds, not by 100
        task = merge_task(branches=(0, 3))
        self.assertEqual(len(task.get_branch_map()), 2)
        self.assertEqual(task.required_runprod_branches(), list(range(50)))

    def test_the_union_covers_the_seeds_each_selected_group_merges(self):
        task = merge_task(branches=((0, 12),))
        required = set(task.required_runprod_branches())
        index = task._runprod_index()
        for era, pi, _, _, seeds in task.get_branch_map().values():
            for seed in seeds:
                self.assertIn(index[(era, pi, seed)], required)
        # 12 groups over two points and two versions: 150 + 150 seeds, each counted once
        self.assertEqual(len(required), 2 * SEEDS_PER_POINT)


class TestClassifyGroup(unittest.TestCase):
    """Readiness of one merge group, from the three directory listings of its point."""

    @staticmethod
    def listing(merged=(), records=(), staged=()):
        return merge_status.Listing(
            merged=set(merged), records=set(records), staged=set(staged)
        )

    @staticmethod
    def names(version, seeds, suffix):
        return [f"nano_{version}_{seed}.{suffix}" for seed in seeds]

    def test_ready_when_every_seed_is_recorded_and_staged(self):
        seeds = list(range(1, 51))
        state, missing, gone = merge_status.classify_group(
            "v12",
            0,
            seeds,
            self.listing(
                records=self.names("v12", seeds, "json"),
                staged=self.names("v12", seeds, "root"),
            ),
        )
        self.assertEqual((state, missing, gone), (merge_status.READY, 0, 0))

    def test_blocked_reports_how_many_seeds_are_owed(self):
        seeds = list(range(1, 51))
        state, missing, gone = merge_status.classify_group(
            "v12",
            0,
            seeds,
            self.listing(
                records=self.names("v12", seeds[:38], "json"),
                staged=self.names("v12", seeds[:38], "root"),
            ),
        )
        self.assertEqual((state, missing), (merge_status.BLOCKED, 12))
        self.assertEqual(gone, 12)

    def test_merged_is_decided_on_the_merged_file_alone(self):
        # the normal state after a successful merge: the records are kept and the staged inputs
        # are deliberately gone. Reading the seeds first would call every merged group broken.
        seeds = list(range(1, 51))
        state, _, _ = merge_status.classify_group(
            "v12",
            3,
            seeds,
            self.listing(
                merged=["nano_v12_3.root"],
                records=self.names("v12", seeds, "json"),
            ),
        )
        self.assertEqual(state, merge_status.MERGED)

    def test_broken_when_a_recorded_seed_lost_its_staged_file(self):
        # `NanoMergeTask.run()` refuses exactly this group, so the report must not call it ready
        seeds = list(range(1, 51))
        state, missing, gone = merge_status.classify_group(
            "v12",
            0,
            seeds,
            self.listing(
                records=self.names("v12", seeds, "json"),
                staged=self.names("v12", seeds[:-1], "root"),
            ),
        )
        self.assertEqual((state, missing, gone), (merge_status.BROKEN, 0, 1))

    def test_a_group_is_read_per_version(self):
        # the two versions of a point share a directory level but not their files: v15 group 0
        # is not merged just because v12 group 0 is
        seeds = list(range(1, 51))
        listing = self.listing(merged=["nano_v12_0.root"])
        self.assertEqual(
            merge_status.classify_group("v12", 0, seeds, listing)[0],
            merge_status.MERGED,
        )
        self.assertEqual(
            merge_status.classify_group("v15", 0, seeds, listing)[0],
            merge_status.BLOCKED,
        )

    def test_empty_storage_is_flagged_as_possibly_unreachable(self):
        seeds = list(range(1, 51))
        blocked = merge_status.Group(
            branch=0,
            era=ERA,
            point=FIRST_POINT,
            version="v12",
            group=0,
            n_seeds=len(seeds),
            state=merge_status.BLOCKED,
            n_missing=len(seeds),
            n_gone=len(seeds),
        )
        self.assertTrue(merge_status.storage_looks_empty([blocked]))
        # a single record is enough to prove the endpoint answers
        self.assertFalse(
            merge_status.storage_looks_empty([blocked._replace(n_missing=49)])
        )


class TestClassifyAgainstStorage(unittest.TestCase):
    """The report over a storage tree, through the real branch map and the real targets."""

    POINTS = ("*_M-250",)

    def setUp(self):
        self.task = NanoMergeTask(
            setup=SETUP, eras=(ERA,), points=self.POINTS, workflow="local"
        )
        self.store = os.path.join(_tmp, "store", "XHHbbWW")
        shutil.rmtree(self.store, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.store, True)

    def put(self, *parts):
        path = os.path.join(self.store, *parts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write("x")

    def stage(self, version, point, seeds, records=True, staged=True):
        for seed in seeds:
            if records:
                self.put(
                    "produced",
                    f"nanoAOD_{version}",
                    ERA,
                    point,
                    f"nano_{version}_{seed}.json",
                )
            if staged:
                self.put(
                    "staging",
                    f"nanoAOD_{version}",
                    ERA,
                    point,
                    f"nano_{version}_{seed}.root",
                )

    def states(self):
        return {g.branch: g.state for g in merge_status.classify(self.task, threads=4)}

    def test_the_selected_points_give_twelve_groups(self):
        # two points (both final states of M-250), 150 seeds each, 3 groups per nano version
        self.assertEqual(len(self.task.get_branch_map()), 12)

    def test_one_ready_group_among_unproduced_ones(self):
        # merge branch 1 = first point, v12, group 1 -> seeds 51-100
        self.stage("v12", FIRST_POINT, range(51, 101))
        states = self.states()
        self.assertEqual(states[1], merge_status.READY)
        self.assertEqual([b for b, s in states.items() if s == merge_status.READY], [1])

    def test_a_merged_group_is_not_reported_again(self):
        self.put("nanoAOD_v12", ERA, FIRST_POINT, "nano_v12_0.root")
        self.assertEqual(self.states()[0], merge_status.MERGED)

    def test_a_seed_short_of_a_group_blocks_it(self):
        self.stage("v12", FIRST_POINT, range(51, 100))
        self.assertEqual(self.states()[1], merge_status.BLOCKED)

    def test_a_lost_staged_file_is_reported_as_broken_not_ready(self):
        self.stage("v12", FIRST_POINT, range(51, 101))
        os.remove(
            os.path.join(
                self.store,
                "staging",
                "nanoAOD_v12",
                ERA,
                FIRST_POINT,
                "nano_v12_60.root",
            )
        )
        self.assertEqual(self.states()[1], merge_status.BROKEN)

    def test_the_printed_command_selects_exactly_the_ready_groups(self):
        # first point v12 groups 0 and 1 ready (seeds 1-100), second point v15 group 0 ready
        self.stage("v12", FIRST_POINT, range(1, 101))
        self.stage("v15", "GluGlutoRadiontoHHto2B2Vto2B2L2Nu_M-250", range(1, 51))
        args = Namespace(
            setup=SETUP, eras=ERA, points=",".join(self.POINTS), test=0, all=False
        )
        groups = merge_status.classify(self.task, threads=4)
        command = merge_status.merge_command(args, groups)
        self.assertIn("--branches 0:2,9", command)
        self.assertIn(f"--points '{self.POINTS[0]}'", command)

        restricted = NanoMergeTask(
            setup=SETUP,
            eras=(ERA,),
            points=self.POINTS,
            workflow="local",
            branches=((0, 2), 9),
        )
        self.assertEqual(
            sorted(restricted.get_branch_map()),
            sorted(g.branch for g in groups if g.state == merge_status.READY),
        )

    def test_a_broken_group_makes_the_report_exit_nonzero(self):
        self.stage("v12", FIRST_POINT, range(1, 51), staged=False)
        args = Namespace(
            setup=SETUP, eras=ERA, points=",".join(self.POINTS), test=0, all=False
        )
        out = StringIO()
        code = merge_status.report(
            args, merge_status.classify(self.task, threads=4), out=out
        )
        self.assertEqual(code, 1)
        self.assertIn("broken 1", out.getvalue())
        self.assertIn("nothing is ready to merge", out.getvalue())


class Sentinel(Exception):
    """Raised in place of the first real work `RunProd.run()` does after its guard."""


class TestRunProdRefusesToGenerateForAnotherTask(unittest.TestCase):
    """A merge job must never spend its slot generating a seed it is only waiting for.

    Now that `NanoMergeTask` requires single seeds rather than the whole workflow, a merge branch
    whose seed is missing is a requirement luigi will happily run inside the merge job -- a 7 h
    chain in a 3 h slot on one core. The decision is made from what `law run` was launched for,
    which is the only thing that tells "this is what was submitted" from "this is a requirement
    of what was submitted" on a worker.
    """

    def runprod_branch(self):
        return RunProd(
            setup=SETUP, eras=(ERA,), points=("*_M-1200",), branch=3, workflow="crab"
        )

    def run_guarded(self, task):
        """Run `task`, with the first step after the guard replaced by `Sentinel`."""
        with mock.patch.object(task.process, "gen_fragment", side_effect=Sentinel):
            task.run()

    def test_a_merge_job_refuses_to_produce_the_seed_it_waits_for(self):
        task = self.runprod_branch()
        with mock.patch.dict(os.environ, {"LAW_JOB_HOME": "/tmp"}):
            with CmdlineParser.global_instance(
                ["NanoMergeTask", "--setup", SETUP], allow_override=True
            ):
                with self.assertRaises(RuntimeError) as caught:
                    self.run_guarded(task)
        message = str(caught.exception)
        self.assertIn("NanoMergeTask", message)
        self.assertIn("must not generate a sample on its own slot", message)
        self.assertIn("run_tools/merge_status.py", message)

    def test_a_runprod_job_generates_normally(self):
        task = self.runprod_branch()
        with mock.patch.dict(os.environ, {"LAW_JOB_HOME": "/tmp"}):
            with CmdlineParser.global_instance(
                ["RunProd", "--setup", SETUP], allow_override=True
            ):
                with self.assertRaises(Sentinel):
                    self.run_guarded(task)

    def test_a_run_law_cannot_attribute_is_allowed(self):
        # no command line to read: blocking here would break a legitimate run for nothing
        task = self.runprod_branch()
        with mock.patch.dict(os.environ, {"LAW_JOB_HOME": "/tmp"}):
            with self.assertRaises(Sentinel):
                self.run_guarded(task)

    def test_a_local_merge_may_still_produce_a_missing_seed(self):
        # `law run NanoMergeTask --workflow local` on the submitting machine is exactly how a
        # small production is finished; only a batch slot is the wrong place to generate
        task = self.runprod_branch()
        with mock.patch.dict(os.environ):
            os.environ.pop("LAW_JOB_HOME", None)
            with CmdlineParser.global_instance(
                ["NanoMergeTask", "--setup", SETUP], allow_override=True
            ):
                with self.assertRaises(Sentinel):
                    self.run_guarded(task)


if __name__ == "__main__":
    unittest.main()
