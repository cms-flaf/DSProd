#!/usr/bin/env python3
"""The merged-file contract is checked rather than assumed.

A merged file holds `events_per_job x files_per_merge` events -- one file per 50 000, to match the
HLepRare skims. Nothing enforced it. The granularity validator divided by the setup scalar while
the seeds were cut with each point's own `events_per_job`, so a per-point override delivered merged
files of a size no one had validated; and the merge only checked its output against the sum of its
own inputs, which is just as true of a group of short files.
"""

import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

# the process plugins live under `<ANALYSIS_PATH>/models`, and the checkout is the area
os.environ["ANALYSIS_PATH"] = dsprod_repo

import law  # noqa: E402
import yaml  # noqa: E402

from dsprod.tasks import CollectGridpacks, NanoMergeTask, Task  # noqa: E402

ERAS = ["Run3_2023", "Run3_2023BPix", "Run3_2024"]
POINT = "GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-250"

_tmp = None
_fs_patcher = None
_env_before = {}


def setUpModule():
    """Point `fs_default` at a local directory, so no test needs a VOMS proxy or the endpoint."""
    global _tmp, _fs_patcher
    _env_before["ANALYSIS_DATA_PATH"] = os.environ.get("ANALYSIS_DATA_PATH")
    _tmp = tempfile.mkdtemp(prefix="dsprod_sizing_test_")
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


def clear_setup_cache():
    """Empty `Task`'s process-wide setup cache.

    A `Task` loads one setup per process and refuses a second path, so a test that brings its own
    setup must leave the cache empty for whatever runs next -- including the other test modules.
    """
    Task.setup_path = None
    Task.prod_setup = None
    Task.conditions = None
    Task.process = None
    Task.all_points = None


class SetupCase(unittest.TestCase):
    """Base for tests that write their own setup and load it through the real `Task.__init__`."""

    #: the shape of the setup each test writes; a subclass narrows it
    events_per_job = 1000
    files_per_merge = 50
    events_total = {"Run3_2023": 100000, "Run3_2023BPix": 50000, "Run3_2024": 50000}

    def setUp(self):
        clear_setup_cache()
        self.addCleanup(clear_setup_cache)
        self.dir = tempfile.mkdtemp(dir=_tmp)
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.n_setups = 0

    def write_setup(self, points=None, **overrides):
        """A real setup YAML on disk; `overrides` replace its top-level fields.

        Every call gets its own file name: luigi caches task instances by their parameters, so
        two setups written to one path would hand back the first task -- branch map included.
        """
        cfg = {
            "process": "X_HH",
            # absolute, so nothing here depends on how `to_abs` resolves a relative path
            "conditions": os.path.join(dsprod_repo, "config", "conditions_Run3.yaml"),
            "output": "sizing",
            "eras": list(ERAS),
            "nano_versions": {"default": ["v12"], "Run3_2024": ["v15"]},
            "first_step": "LHEGS",
            "last_step": "NANO",
            "events_per_job": self.events_per_job,
            "files_per_merge": self.files_per_merge,
            "production_mode": "GluGlutoRadion",
            "points": points
            or [
                {
                    "name": POINT,
                    "mass": 250,
                    "spin": 0,
                    "final_state": "2B2JLNu",
                    "events_total": dict(self.events_total),
                }
            ],
        }
        cfg.update(overrides)
        self.n_setups += 1
        path = os.path.join(self.dir, f"sizing{self.n_setups}.yaml")
        with open(path, "w") as f:
            yaml.safe_dump(cfg, f)
        return path

    def task(self, cls=CollectGridpacks, points=None, setup=None, **kwargs):
        return cls(setup=setup or self.write_setup(points=points), **kwargs)

    def point(self, **overrides):
        """The setup's single point, with `overrides` applied."""
        cfg = {
            "name": POINT,
            "mass": 250,
            "spin": 0,
            "final_state": "2B2JLNu",
            "events_total": dict(self.events_total),
        }
        cfg.update(overrides)
        return cfg


class TestMergeGranularityValidator(SetupCase):
    """What `_validate_merge_granularity` refuses, and what it used to let through."""

    def test_a_sample_that_does_not_fill_whole_files_is_refused(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.task(points=[self.point(events_total={"Run3_2023": 120000})])
        msg = str(ctx.exception)
        self.assertIn(POINT, msg)
        self.assertIn("Run3_2023", msg)
        self.assertIn("50000", msg, "the message must name the file size it wanted")

    def test_a_per_point_events_per_job_override_is_refused(self):
        # the escape: the validator multiplied the *setup* scalar (1000 x 50 = 50 000) while the
        # seeds were cut with the point's own 500, so this setup passed and then delivered merged
        # files of 500 x 50 = 25 000 events -- half the size its own comment advertises
        self.assertEqual(100000 % (self.events_per_job * self.files_per_merge), 0)
        with self.assertRaises(RuntimeError) as ctx:
            self.task(
                points=[
                    self.point(events_per_job=500, events_total={"Run3_2023": 100000})
                ]
            )
        msg = str(ctx.exception)
        self.assertIn(POINT, msg)
        self.assertIn("500", msg)
        self.assertIn("1000", msg)

    def test_an_era_a_point_does_not_produce_is_not_checked(self):
        # `events_total` covers every era of the setup, with 0 where a point is not produced
        self.task(
            points=[self.point(events_total={"Run3_2023": 100000, "Run3_2024": 0})]
        )

    def test_test_mode_is_exempt(self):
        # `--test` deliberately runs one short job per point and era, and replaces every point's
        # size with the requested event count -- which is exactly what the new check refuses
        self.task(test=100, points=[self.point(events_total={"Run3_2023": 120000})])


class TestMergedSizeContract(SetupCase):
    """`NanoMergeTask.run` must land a merged file of the size the setup promises.

    Six seeds in two groups of three, so a whole group's records and staged files fit in a test.
    """

    events_per_job = 1000
    files_per_merge = 3
    events_total = {"Run3_2023": 6000}

    ERA = "Run3_2023"
    VERSION = "v12"

    def setUp(self):
        super(TestMergedSizeContract, self).setUp()
        self.store = os.path.join(_tmp, "store", "sizing")
        shutil.rmtree(self.store, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.store, True)
        self.hadd = mock.patch("dsprod.run_step.hadd_nano", side_effect=self.fake_hadd)
        self.hadd_nano = self.hadd.start()
        self.addCleanup(self.hadd.stop)

    @staticmethod
    def fake_hadd(params, out_path, in_paths, work_dir):
        with open(out_path, "w") as f:
            f.write("merged")

    def branch_task(self, **kwargs):
        """Merge branch 0: seeds 1-3 of the only point, in the only nano version."""
        kwargs.setdefault("eras", (self.ERA,))
        task = self.task(cls=NanoMergeTask, workflow="local", branch=0, **kwargs)
        self.assertEqual(task.branch_data[3], 0)
        return task

    def produce(self, task, seeds, requested=None):
        """Stage a nano file and write a `produced/` record for each seed, as `RunProd` does."""
        _, pi, version, _, _ = task.branch_data
        point = task.prod_points[pi]
        for seed in seeds:
            task.staged_nano_target(self.ERA, point, version, seed).touch()
            task._write_produced_record(
                self.ERA,
                point,
                version,
                seed,
                (requested or {}).get(seed, int(task.prod_setup["events_per_job"])),
            )

    def counts(self, merged, per_input):
        """`run_step.count_events` over one merge: the merged file first, then each input."""
        return mock.patch(
            "dsprod.run_step.count_events",
            side_effect=[merged] + [per_input] * self.files_per_merge,
        )

    def staged(self, task):
        _, pi, version, _, seeds = task.branch_data
        point = task.prod_points[pi]
        return [
            task.staged_nano_target(self.ERA, point, version, seed) for seed in seeds
        ]

    def test_a_group_at_the_contracted_size_merges_and_drops_its_inputs(self):
        task = self.branch_task()
        self.produce(task, (1, 2, 3))
        with self.counts(3000, 1000):
            task.run()
        self.assertTrue(task.output().exists())
        self.assertEqual([t.exists() for t in self.staged(task)], [False] * 3)

    def test_a_mixed_size_group_is_refused_before_it_is_merged(self):
        # the case `events_requested` was written for and never read: seed 2 predates a change of
        # `events_per_job`, and merging it would deliver 2500 events in a 3000-event file
        task = self.branch_task()
        self.produce(task, (1, 2, 3), requested={2: 500})
        with self.counts(3000, 1000):
            with self.assertRaises(RuntimeError) as ctx:
                task.run()
        msg = str(ctx.exception)
        self.assertIn("seed 2: 500", msg)
        self.assertIn("1000", msg)
        self.hadd_nano.assert_not_called()
        # nothing merged, so nothing may be dropped either
        self.assertEqual([t.exists() for t in self.staged(task)], [True] * 3)
        self.assertFalse(task.output().exists())

    def test_a_group_one_event_short_still_merges(self):
        # the real case the contract is deliberately NOT checked against: a Run3_2023 job returned
        # 999 of its 1000 events, so that merged file holds 49 999. Refusing the group over one
        # event would strand it -- re-running the seed yields 999 again -- and `n_out == sum(n_in)`
        # still proves the merge itself lost nothing
        task = self.branch_task()
        self.produce(task, (1, 2, 3))
        with mock.patch(
            "dsprod.run_step.count_events", side_effect=[2999, 1000, 1000, 999]
        ):
            task.run()
        self.assertTrue(task.output().exists())
        self.assertEqual([t.exists() for t in self.staged(task)], [False] * 3)

    def test_a_merge_that_loses_a_file_is_still_reported_as_a_mismatch(self):
        task = self.branch_task()
        self.produce(task, (1, 2, 3))
        with self.counts(2000, 1000):
            with self.assertRaises(RuntimeError) as ctx:
                task.run()
        self.assertIn("sum inputs", str(ctx.exception))

    def test_a_test_run_merges_its_single_short_seed(self):
        # `--test` contracts one job of `--test` events, so the check must not read the setup
        task = self.branch_task(test=100)
        self.assertEqual(task.branch_data[4], [1])
        self.produce(task, (1,))
        with mock.patch("dsprod.run_step.count_events", side_effect=[100, 100]):
            task.run()
        self.assertTrue(task.output().exists())

    def test_a_record_that_does_not_say_its_size_names_the_seed(self):
        # an interrupted upload leaves a zero-byte record; the bare JSONDecodeError it used to
        # raise named neither the seed nor the file, in the one method whose neighbouring error
        # prints the offending path
        task = self.branch_task()
        self.produce(task, (1, 2, 3))
        _, pi, version, _, _ = task.branch_data
        record = task.produced_nano_target(self.ERA, task.prod_points[pi], version, 2)
        with open(record.abspath, "w"):
            pass
        with self.counts(3000, 1000):
            with self.assertRaises(RuntimeError) as ctx:
                task.run()
        msg = str(ctx.exception)
        self.assertIn("seed 2", msg)
        self.assertIn(record.path, msg)
        self.hadd_nano.assert_not_called()
        self.assertEqual([t.exists() for t in self.staged(task)], [True] * 3)

    def test_a_record_without_the_field_names_the_seed_too(self):
        task = self.branch_task()
        self.produce(task, (1, 2, 3))
        _, pi, version, _, _ = task.branch_data
        record = task.produced_nano_target(self.ERA, task.prod_points[pi], version, 3)
        record.dump({"seed": 3}, formatter="json")
        with self.counts(3000, 1000):
            with self.assertRaises(RuntimeError) as ctx:
                task.run()
        self.assertIn("seed 3", str(ctx.exception))
        self.assertIn("events_requested", str(ctx.exception))

    def test_the_records_are_the_ones_runprod_declares(self):
        # `_check_contracted_inputs` reads `self.input()`, so a group's records must be exactly
        # the outputs of the RunProd branches it requires -- no second path built by hand
        task = self.branch_task()
        _, pi, version, _, seeds = task.branch_data
        point = task.prod_points[pi]
        for seed in seeds:
            self.assertEqual(
                task.input()[seed][version].path,
                task.produced_nano_target(self.ERA, point, version, seed).path,
            )


if __name__ == "__main__":
    unittest.main()
