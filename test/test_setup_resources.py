#!/usr/bin/env python3
"""Runtime, memory and cores as a property of the production, not of DSProd.

Every setup produces different jobs -- one model's gridpack takes minutes where another's takes
hours, and a denser era needs more memory per event -- so what a job asks its batch system for
belongs next to the points it describes. A setup's `resources:` block therefore enters luigi's
config layer, which sits between a parameter's default and the command line.

What these tests pin down is the precedence and the silence of neither end of it: a setup beats
DSProd's class default, the command line beats the setup, a task the setup does not mention keeps
its default, and a setup that names a task or a key that does not exist is refused rather than
quietly ignored -- a resource request nobody applies is exactly the kind of change that is only
noticed when the jobs come back killed.
"""

import contextlib
import os
import sys
import tempfile
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

# ... and the test directory itself, so a sibling module is importable by its bare name whether the
# suite is started from the repository root or with `unittest discover -s test`, as CI does. It must
# be the bare name: `test.<module>` resolves to CPython's own `test` package wherever that is
# installed, which is why CI could not import this file while a local run could.
test_dir = os.path.dirname(os.path.abspath(__file__))
if test_dir not in sys.path:
    sys.path.insert(0, test_dir)

os.environ["ANALYSIS_PATH"] = dsprod_repo

import luigi  # noqa: E402
import luigi.configuration  # noqa: E402
from luigi.cmdline_parser import CmdlineParser  # noqa: E402

import law  # noqa: E402

law.contrib.load("cms")

from dsprod import tasks  # noqa: E402
from dsprod.crab import CrabWorkflow  # noqa: E402
from dsprod.tasks import MakeGridpack, NanoMergeTask, RunProd  # noqa: E402

# law's job-config skeleton, already built for the CRAB resource tests
from test_crab_resources import Cfg  # noqa: E402

#: the production setup this checkout ships, used for the end-to-end case
SETUP = "models/X_HH/setups/Run3_XHHbbWW.yaml"


def resolved(task_cls, name):
    """The value luigi would give `<task>.<name>` right now, by its own resolution order."""
    param = dict(task_cls.get_params())[name]
    return param.task_value(task_cls.get_task_family(), name)


@contextlib.contextmanager
def written(resources, load=True):
    """A setup carrying this `resources:` block, read the way a run reads one.

    luigi's config is process-wide, so the block is taken back out again afterwards: a test that
    left its section behind would set the resources of every test that runs after it. The setups
    this checkout ships are reloaded by `restore_shipped_resources` for the same reason.
    """
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "setup.yaml")
        with open(path, "w") as f:
            f.write("process: X_HH\nresources:\n")
            for family, requests in resources.items():
                f.write(f"  {family}:\n")
                for name, value in requests.items():
                    f.write(f"    {name}: {value}\n")
        cfg = luigi.configuration.get_config()
        try:
            if load:
                tasks.load_setup(path)
            yield path
        finally:
            tasks._setup_cache.pop(path, None)
            for family in resources:
                cfg.remove_section(family)


def restore_shipped_resources():
    """Re-apply the resources of every setup already read in this process.

    Removing a section removes whatever another test's setup put there, and `load_setup` caches
    the parse, so the value has to be put back rather than waiting to be re-read.
    """
    for path in list(tasks._setup_cache):
        tasks.load_setup(path)


class WithNoSetupRead(unittest.TestCase):
    """Base for the tests that measure DSProd's own defaults.

    A setup another test module read is still in luigi's config -- one law process runs one
    production, so nothing puts it back -- and these tests start from before that.
    """

    def setUp(self):
        cfg = luigi.configuration.get_config()
        for family in ("RunProd", "MakeGridpack", "NanoMergeTask"):
            cfg.remove_section(family)
        self.addCleanup(restore_shipped_resources)


class TheSetupSetsWhatAJobAsksFor(WithNoSetupRead):

    def test_a_setup_overrides_the_class_default(self):
        self.assertEqual(resolved(RunProd, "max_runtime"), 24.0)
        with written({"RunProd": {"max_runtime": 16, "memory": 8000, "n_cpus": 2}}):
            self.assertEqual(resolved(RunProd, "max_runtime"), 16.0)
            self.assertEqual(resolved(RunProd, "memory"), 8000)
            self.assertEqual(resolved(RunProd, "n_cpus"), 2)
        self.assertEqual(resolved(RunProd, "max_runtime"), 24.0)

    def test_a_duration_is_read_the_way_the_command_line_reads_it(self):
        """`16` and `16h` are the same sixteen hours, and `90m` is an hour and a half."""
        with written({"RunProd": {"max_runtime": "16h"}}):
            self.assertEqual(resolved(RunProd, "max_runtime"), 16.0)
        with written({"RunProd": {"max_runtime": "90m"}}):
            self.assertEqual(resolved(RunProd, "max_runtime"), 1.5)

    def test_a_task_the_setup_leaves_out_keeps_its_default(self):
        with written({"RunProd": {"max_runtime": 16}}):
            self.assertEqual(resolved(NanoMergeTask, "max_runtime"), 3.0)
            self.assertEqual(resolved(MakeGridpack, "max_runtime"), 12.0)

    def test_a_setup_without_resources_changes_nothing(self):
        self.assertEqual(tasks.setup_resources({"process": "X_HH"}, "s.yaml"), [])
        self.assertEqual(resolved(RunProd, "max_runtime"), 24.0)
        self.assertEqual(resolved(RunProd, "n_cpus"), 4)


class ButTheCommandLineStillWins(WithNoSetupRead):
    """The point of using luigi's config layer rather than patching the task afterwards."""

    def test_an_option_on_the_command_line_beats_the_setup(self):
        with written({"RunProd": {"max_runtime": 16, "n_cpus": 2}}):
            with CmdlineParser.global_instance(
                ["RunProd", "--RunProd-max-runtime", "30h", "--setup", SETUP]
            ):
                self.assertEqual(resolved(RunProd, "max_runtime"), 30.0)
                # and only that one: the rest of the block still applies
                self.assertEqual(resolved(RunProd, "n_cpus"), 2)

    def test_the_setup_is_found_when_it_comes_from_the_command_line(self):
        """`law run ... --RunProd-setup <path>`, where the path is not a keyword argument.

        The bare `--setup <path>` of a run's root task is handed to the constructor by luigi's own
        interface and so arrives as one (the test below); this scoped form is resolved by the
        parameter itself, and reading the setup from the keyword arguments alone would leave a run
        given it this way with no resources applied at all.
        """
        with written({"RunProd": {"n_cpus": 2}}, load=False) as path:
            with CmdlineParser.global_instance(["RunProd", "--RunProd-setup", path]):
                values = dict(RunProd.get_param_values(RunProd.get_params(), (), {}))
            self.assertEqual(values["n_cpus"], 2)

    def test_a_value_passed_to_the_task_beats_the_setup(self):
        """How law converts a workflow into its branches: the values go in as keyword arguments."""
        with written({"RunProd": {"max_runtime": 16, "n_cpus": 2}}, load=False) as path:
            values = dict(
                RunProd.get_param_values(
                    RunProd.get_params(), (), {"setup": path, "max_runtime": 30.0}
                )
            )
            self.assertEqual(values["max_runtime"], 30.0)
            # the setup was read all the same -- the keyword argument wins only its own key
            self.assertEqual(values["n_cpus"], 2)


class AMistakeInTheBlockIsRefused(WithNoSetupRead):

    def test_a_task_that_does_not_exist(self):
        with self.assertRaises(RuntimeError) as caught:
            tasks.setup_resources(
                {"resources": {"RunPord": {"max_runtime": 16}}}, "s.yaml"
            )
        self.assertIn("RunPord", str(caught.exception))
        # the message says what can be set instead of leaving the author to guess
        self.assertIn("RunProd", str(caught.exception))

    def test_a_parameter_that_is_not_a_resource(self):
        with self.assertRaises(RuntimeError) as caught:
            tasks.setup_resources({"resources": {"RunProd": {"retries": 3}}}, "s.yaml")
        self.assertIn("retries", str(caught.exception))
        self.assertEqual(resolved(RunProd, "retries"), 9)

    def test_a_task_that_runs_locally(self):
        """`ImportGridpack` has no batch system to ask, so requesting cores for it is a mistake."""
        with self.assertRaises(RuntimeError) as caught:
            tasks.setup_resources(
                {"resources": {"ImportGridpack": {"n_cpus": 4}}}, "s.yaml"
            )
        self.assertIn("ImportGridpack", str(caught.exception))

    def test_a_shared_base_class_rather_than_a_task(self):
        """`HTCondorWorkflow` carries all three settings and is nobody's task family.

        luigi resolves a config section per concrete task, so a request written against the base
        would be accepted and then apply to nothing -- the silent no-op this block must not have.
        """
        with self.assertRaises(RuntimeError) as caught:
            tasks.setup_resources(
                {"resources": {"HTCondorWorkflow": {"max_runtime": 10}}}, "s.yaml"
            )
        self.assertIn("HTCondorWorkflow", str(caught.exception))
        self.assertEqual(resolved(RunProd, "max_runtime"), 24.0)

    def test_a_value_the_setting_cannot_take(self):
        """Refused while reading the setup, where the message can name it, rather than later
        inside luigi where the error names neither the setup nor the task."""
        for name, value in (
            ("max_runtime", "soon"),
            ("n_cpus", "four"),
            ("memory", "lots"),
        ):
            with self.assertRaises(RuntimeError) as caught:
                tasks.setup_resources(
                    {"resources": {"RunProd": {name: value}}}, "s.yaml"
                )
            self.assertIn(name, str(caught.exception))

    def test_a_value_that_asks_for_nothing(self):
        """A setup says "no particular requirement" by leaving the key out, not by writing 0.

        Zero is not a request: on CRAB a zero runtime means no limit at all, on HTCondor it becomes
        a negative one, and a zero memory or core count is the framework's own marker for "work it
        out" -- so a setup carrying one is a mistake with a silent consequence.
        """
        for name, value in (
            ("memory", 0),
            ("n_cpus", 0),
            ("max_runtime", 0),
            ("memory", -1),
        ):
            with self.assertRaises(RuntimeError) as caught:
                tasks.setup_resources(
                    {"resources": {"RunProd": {name: value}}}, "s.yaml"
                )
            self.assertIn(name, str(caught.exception))
            # and it says what to do instead
            self.assertIn("Leave the key out", str(caught.exception))

    def test_a_task_may_still_name_only_some_of_them(self):
        """Dropping a key is how a setup defers, so it has to keep working."""
        with written({"NanoMergeTask": {"max_runtime": 5}}):
            self.assertEqual(resolved(NanoMergeTask, "max_runtime"), 5.0)
            self.assertEqual(resolved(NanoMergeTask, "memory"), 5000)
            self.assertEqual(resolved(NanoMergeTask, "n_cpus"), 1)

    def test_a_resources_block_that_is_not_a_mapping(self):
        with self.assertRaises(RuntimeError):
            tasks.setup_resources({"resources": 5}, "s.yaml")

    def test_a_value_that_is_not_a_number(self):
        for value in (None, True, ["16"]):
            with self.assertRaises(RuntimeError):
                tasks.setup_resources(
                    {"resources": {"RunProd": {"max_runtime": value}}}, "s.yaml"
                )

    def test_the_bad_block_leaves_nothing_behind(self):
        """A refusal must not half-apply: the first entry of a block cannot survive the second."""
        with self.assertRaises(RuntimeError):
            tasks.setup_resources(
                {
                    "resources": {
                        "RunProd": {"max_runtime": 16},
                        "RunPord": {"n_cpus": 2},
                    }
                },
                "s.yaml",
            )
        self.assertEqual(resolved(RunProd, "max_runtime"), 24.0)


class ThroughTheRealTask(WithNoSetupRead):
    def setUp(self):
        # and the parse cache with it, or a run that never read the setup would still see it
        super(ThroughTheRealTask, self).setUp()
        tasks._setup_cache.pop(tasks.to_abs_path(SETUP), None)

    def test_reading_a_setup_applies_its_resources(self):
        with written({"MakeGridpack": {"max_runtime": 6, "n_cpus": 1}}) as path:
            tasks.load_setup(path)
            self.assertEqual(resolved(MakeGridpack, "max_runtime"), 6.0)
            self.assertEqual(resolved(MakeGridpack, "n_cpus"), 1)

    def test_a_real_task_is_built_with_the_numbers_of_its_setup(self):
        """The whole chain, through luigi's own command line: `law run RunProd --setup <this>`
        builds a task that runs 16 h on 4 cores."""
        if not os.path.exists(tasks.to_abs_path(SETUP)):
            self.skipTest("models submodule not checked out")
        argv = [
            "RunProd",
            "--setup",
            SETUP,
            "--eras",
            "Run3_2023BPix",
            "--points",
            "*_M-250",
            "--workflow",
            "local",
        ]
        with CmdlineParser.global_instance(argv):
            task = CmdlineParser.get_instance().get_task_obj()
        self.assertEqual(task.max_runtime, 16.0)
        self.assertEqual(task.memory, 10000)
        self.assertEqual(task.n_cpus, 4)
        # and the same values are what law hands a branch or a submitted job, because they were
        # resolved as parameters rather than set on the task afterwards
        self.assertEqual(task.param_kwargs["max_runtime"], 16.0)

    def test_the_merge_does_not_impose_its_resources_on_what_it_requires(self):
        """The incident this would repeat: law copies parameter values through `req()`, and
        `NanoMergeTask`'s 3 h and one core once forced its `RunProd` requirement to run in 3 h on
        one core -- 91 % of a 3270-job CRAB task was killed on walltime. `exclude_params_req` is
        what fixed it; now that a setup names the resources of BOTH tasks, the same question has
        to be asked again of the values the setup supplies.
        """
        if not os.path.exists(tasks.to_abs_path(SETUP)):
            self.skipTest("models submodule not checked out")
        argv = [
            "NanoMergeTask",
            "--setup",
            SETUP,
            "--eras",
            "Run3_2023BPix",
            "--points",
            "*_M-250",
            "--workflow",
            "local",
        ]
        with CmdlineParser.global_instance(argv):
            merge = CmdlineParser.get_instance().get_task_obj()
        self.assertEqual(
            (merge.max_runtime, merge.memory, merge.n_cpus), (3.0, 5000, 1)
        )
        produce = merge.workflow_requires()["runprod"]
        self.assertEqual(
            (produce.max_runtime, produce.memory, produce.n_cpus), (16.0, 10000, 4)
        )
        # and from a merge *branch*, which is the path the incident took
        branch = merge.as_branch(0)
        self.assertEqual(
            (branch.max_runtime, branch.memory, branch.n_cpus), (3.0, 5000, 1)
        )
        seed = list(branch.requires().values())[0]
        self.assertEqual((seed.max_runtime, seed.memory, seed.n_cpus), (16.0, 10000, 4))

    def test_what_crab_is_actually_told(self):
        """The end of the chain: the setup's numbers as they reach the batch system.

        Everything above tests parameter values; this is the submitted configuration built from
        them, which is what a site matches on and what kills a job that exceeds it.
        """
        if not os.path.exists(tasks.to_abs_path(SETUP)):
            self.skipTest("models submodule not checked out")
        expected = {
            "RunProd": (4, 10000, 16 * 60),
            # two cores for a single-threaded merge: CRAB sells memory per core, so the 5000 MB
            # the setup asks for is what buys them -- `n_cpus: 1` still states what haddnano uses
            "NanoMergeTask": (2, 5000, 3 * 60),
            # and no memory in the setup leaves the one-core ceiling, rather than a second core
            "MakeGridpack": (1, 3000, 12 * 60),
        }
        for name, (cores, memory, minutes) in expected.items():
            argv = [name, "--setup", SETUP, "--points", "*_M-250", "--workflow", "crab"]
            with CmdlineParser.global_instance(argv):
                task = CmdlineParser.get_instance().get_task_obj()
            cfg = Cfg()
            cls = type(task)
            with mock.patch.object(
                cls, "ana_data_path", lambda self: tempfile.gettempdir()
            ), mock.patch(
                "dsprod.crab.processing_sites", return_value=["T2_X_Y"]
            ), mock.patch.object(
                cls, "_ensure_crab_pset", lambda self, n: "/tmp/pset.py"
            ), mock.patch.object(
                cls, "_code_tarball", lambda self: "/tmp/code.tar.gz"
            ), mock.patch.object(
                cls, "site_stats", lambda self: mock.Mock(blacklist=lambda: [])
            ), mock.patch.object(
                cls, "_crab_cfg", lambda self: {"max_cores": 4}
            ):
                CrabWorkflow.crab_job_config(task, cfg, [1], [0])
            self.assertEqual(cfg.crab.JobType.numCores, cores, name)
            self.assertEqual(cfg.crab.JobType.maxMemoryMB, memory, name)
            self.assertEqual(cfg.crab.JobType.maxJobRuntimeMin, minutes, name)

    def test_the_setup_this_checkout_ships(self):
        """The numbers the production actually runs with, read the way a run reads them."""
        path = tasks.to_abs_path(SETUP)
        if not os.path.exists(path):  # the models submodule is not initialised
            self.skipTest("models submodule not checked out")
        tasks.load_setup(path)
        self.assertEqual(resolved(RunProd, "max_runtime"), 16.0)
        self.assertEqual(resolved(RunProd, "memory"), 10000)
        self.assertEqual(resolved(RunProd, "n_cpus"), 4)
        self.assertEqual(resolved(NanoMergeTask, "max_runtime"), 3.0)
        self.assertEqual(resolved(NanoMergeTask, "memory"), 5000)
        self.assertEqual(resolved(NanoMergeTask, "n_cpus"), 1)
        # the MakeGridpack and NanoMergeTask numbers happen to equal DSProd's own defaults -- this
        # setup declares what the production asks for rather than changing it -- so those lines
        # state the request; what tests the mechanism is RunProd above
        self.assertEqual(resolved(MakeGridpack, "max_runtime"), 12.0)
        self.assertEqual(resolved(MakeGridpack, "n_cpus"), 1)
        # and the setup names no memory for it, so the framework's own marker is what is left:
        # CRAB's max(3000, 2500 * numCores), which is 3000 MB on the one core above
        self.assertEqual(resolved(MakeGridpack, "memory"), 0)


if __name__ == "__main__":
    unittest.main()
