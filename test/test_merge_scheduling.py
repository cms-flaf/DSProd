#!/usr/bin/env python3
"""The merge waited for the whole generation stage, and nothing said which groups could run.

In the Run3_2023BPix production (4800 `RunProd` branches, 192 merge groups) 169 groups were
complete and none had merged, because `NanoMergeTask.workflow_requires()` required the entire
`RunProd` workflow. Narrowing it to the seeds of the groups actually being merged is what these
tests pin down -- including the numbering, since law copies `branches` through `req()` and
`--branches 5` on the merge therefore used to ask for *RunProd* branch 5 rather than for the 50
seeds of merge group 5, and one level further down asked `MakeGridpack` for gridpacks by seed
number.

The report has its own failure to answer for: a listing that fails and a directory that is not
there are the same answer at the gfal layer, and reading the first as the second turned a
delivered point into an instruction to produce its seeds again.
"""

import argparse
import contextlib
import os
import shutil
import sys
import tempfile
import unittest
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
_env_before = {}


def setUpModule():
    """Point `fs_default` at a local directory, so no test needs a VOMS proxy or the endpoint."""
    global _tmp, _fs_patcher
    _env_before["ANALYSIS_DATA_PATH"] = os.environ.get("ANALYSIS_DATA_PATH")
    _tmp = tempfile.mkdtemp(prefix="dsprod_merge_test_")
    os.environ["ANALYSIS_DATA_PATH"] = os.path.join(_tmp, "data")
    _fs_patcher = mock.patch(
        "dsprod.tasks.get_fs",
        return_value=law.LocalFileSystem(base=os.path.join(_tmp, "store")),
    )
    _fs_patcher.start()


def tearDownModule():
    _fs_patcher.stop()
    # restored, or a module discovered after this one inherits a data path in a deleted tmpdir
    for name, value in _env_before.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
    shutil.rmtree(_tmp, ignore_errors=True)


def merge_task(**kwargs):
    """The merge workflow over one full era of the real setup."""
    kwargs.setdefault("workflow", "htcondor")
    return NanoMergeTask(setup=SETUP, eras=(ERA,), **kwargs)


class TestTheSeedSelectionStopsAtRunProd(unittest.TestCase):
    """`RunProd`'s own requirements branch over gridpacks and eras, not over seeds.

    law copies `branches` through `req()`, so a seed range arrived at `MakeGridpack` as a gridpack
    range: `--branches 10:20` asked for gridpacks 10-19 while seed 10 needs gridpack 0, and the
    requirement was then satisfied by a workflow that never builds it -- the branch job would find
    the gridpack missing on the worker and refuse to generate it there. The same range dropped the
    premix list of every era outside it. `req_different_branching` is what stops the copy.
    """

    def runprod(self, **kwargs):
        kwargs.setdefault("eras", (ERA,))
        return RunProd(setup=SETUP, workflow="htcondor", **kwargs)

    def test_a_seed_range_does_not_select_gridpacks(self):
        gridpack = self.runprod(branches=((10, 20),)).workflow_requires()["gridpack"]
        self.assertEqual(gridpack.branches, ())
        self.assertEqual(
            sorted(gridpack.get_branch_map()),
            sorted(self.runprod().workflow_requires()["gridpack"].get_branch_map()),
        )
        # the gridpack seed 10 really needs, which the leaked range 10-19 did not contain
        branch = self.runprod(branch=10)
        _, pi, _ = branch.branch_data
        needed = branch.gridpack_index()[
            branch.process.gridpack_name(branch.prod_points[pi])
        ]
        self.assertEqual(needed, 0)
        self.assertIn(needed, gridpack.get_branch_map())

    def test_a_seed_range_does_not_drop_an_era_from_the_era_wide_requirements(self):
        # over every era of the setup, so the leaked range overlaps these maps rather than falling
        # outside them, which law would collapse back to "all branches"
        selected = RunProd(setup=SETUP, workflow="htcondor", branches=((1, 3),))
        reqs = selected.workflow_requires()
        for name in ("cmssw", "premix"):
            self.assertEqual(reqs[name].branches, ())
            self.assertEqual(len(reqs[name].get_branch_map()), len(selected.prod_eras))

    def test_the_merge_requires_no_part_of_the_generation_stage(self):
        # the strongest form of "a merge selection cannot reach seeds or gridpacks": there is no
        # edge to reach along. `RunProd` is required once by `Produce`, as a whole.
        merge = merge_task(branches=(6, (7, 8)))
        self.assertNotIn("runprod", merge.workflow_requires())
        self.assertEqual(merge.as_branch(6).requires(), {})


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
        self.assertIn("Produce that seed first", message)

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


if __name__ == "__main__":
    unittest.main()
