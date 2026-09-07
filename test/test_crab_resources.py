#!/usr/bin/env python3
"""How many cores and how much memory a CRAB job asks for.

CRAB sells memory only in per-core units -- it refuses any task above
`max(MAX_MEMORY_SINGLE_CORE, numCores * MAX_MEMORY_PER_CORE)` and accepts only 1, 2, 4 or 8 cores
-- so the two knobs cannot be made fully independent. What they can be, and what these tests pin
down, is: an explicit request is honoured exactly; nothing silently raises it (the old arithmetic
forced every request back up to `n_cores * 2500`, which made `--crab-memory` inert); nothing
silently lowers it either, because on CRAB the number is a kill threshold rather than a
reservation, so a shrunk request is a dead branch and an unsatisfiable one has to be an error at
submit time; and a request larger than the task's own cores can hold buys the cores it needs,
which is the only way more memory exists on this backend.
"""

import os
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

import law  # noqa: E402
import law.job.base  # noqa: E402

from dsprod.crab import CrabWorkflow  # noqa: E402


class AutoDot(law.util.DotDict):
    """law's own DotDict, auto-creating sub-sections on first access.

    Deliberately NOT a hand-written copy of law's `crab` skeleton (job.py:572-...): mirroring that
    structure would silently drift from it. This only has to accept whatever the resource block
    writes, so the test asserts on DSProd's choices and never enumerates law's fields.
    """

    def __getattr__(self, attr):
        if attr.startswith("_"):
            raise AttributeError(attr)
        if attr not in self:
            self[attr] = AutoDot()
        return self[attr]


class Cfg(law.job.base.BaseJobFileFactory.Config):
    """law's real config object, with the nested `crab` section law would have put on it."""

    def __init__(self):
        super(Cfg, self).__init__()
        self.crab = AutoDot()
        self.input_files = {}
        self.render_variables = {}
        self.custom_content = []


def resources(crab_cfg, n_cpus=1, memory=0, crab_memory=-1):
    """Run the real `crab_job_config` resource block and return (numCores, maxMemoryMB)."""
    task = mock.Mock()
    task.n_cpus = n_cpus
    task.memory = memory
    task.crab_memory = crab_memory
    task.task_family = "TestTask"
    task.max_runtime = 0
    task.crab_whitelist = ()
    task.crab_blacklist = ()
    task._crab_cfg = lambda: dict(crab_cfg)
    task._ensure_crab_pset = lambda n: f"/tmp/pset_threads{n}.py"
    task._code_tarball = lambda: "/tmp/code.tar.gz"
    task.site_stats = lambda: mock.Mock(blacklist=lambda: [])
    task.ana_data_path = lambda: "/tmp"
    cfg = Cfg()
    # the site-selection tail of the method needs a CMS site list from disk; it is irrelevant here
    with mock.patch("dsprod.crab.processing_sites", return_value=["T2_X_Y"]):
        CrabWorkflow.crab_job_config(task, cfg, [1], [0])
    return cfg.crab.JobType.numCores, cfg.crab.JobType.maxMemoryMB, cfg


class WhatTheJobAsksFor(unittest.TestCase):
    def test_the_production_request_is_unchanged(self):
        """RunProd's resolved values must not move: a live production restarts against this."""
        self.assertEqual(
            resources({"max_cores": 4}, n_cpus=4, memory=10000)[:2], (4, 10000)
        )

    def test_an_unset_memory_still_uses_the_per_core_formula(self):
        self.assertEqual(resources({"max_cores": 4}, n_cpus=4)[:2], (4, 10000))

    def test_a_smaller_explicit_request_is_honoured(self):
        """The regression: the old floor raised every request back to n_cores * 2500."""
        self.assertEqual(
            resources({"max_cores": 4}, n_cpus=4, memory=6000)[:2], (4, 6000)
        )

    def test_the_per_run_override_is_honoured_and_wins(self):
        self.assertEqual(
            resources({"max_cores": 4}, n_cpus=4, memory=6000, crab_memory=7000)[:2],
            (4, 7000),
        )

    def test_memory_buys_the_cores_it_needs(self):
        """A single-threaded task asking 5000 MB: CRAB has no 1-core 5000 MB job."""
        self.assertEqual(
            resources({"max_cores": 4}, n_cpus=1, memory=5000)[:2], (2, 5000)
        )

    def test_a_single_core_job_gets_crabs_single_core_allowance(self):
        self.assertEqual(resources({"max_cores": 4}, n_cpus=1)[:2], (1, 3000))

    def test_an_unsatisfiable_request_is_an_error_not_a_clamp(self):
        """Silently shrinking it would turn a kill threshold into a dead branch."""
        with self.assertRaises(ValueError) as caught:
            resources({"max_cores": 4}, n_cpus=4, memory=12000)
        msg = str(caught.exception)
        self.assertIn("numCores >= 8", msg)
        self.assertIn("max_cores", msg)

    def test_the_same_request_succeeds_when_the_cap_allows_it(self):
        self.assertEqual(
            resources({"max_cores": 8}, n_cpus=4, memory=12000)[:2], (8, 12000)
        )

    def test_core_counts_crab_rejects_are_never_submitted(self):
        """CRAB accepts only 1, 2, 4, 8; a computed 3 or 6 is refused at submit."""
        self.assertEqual(resources({"max_cores": 8}, n_cpus=3)[0], 4)
        self.assertEqual(resources({"max_cores": 6}, n_cpus=6)[0], 4)
        self.assertEqual(resources({"max_cores": 3}, n_cpus=3)[0], 2)

    def test_the_pset_declares_exactly_the_cores_requested(self):
        """The client refuses a task whose pset threads differ from numCores."""
        n_cores, _, cfg = resources({"max_cores": 4}, n_cpus=1, memory=5000)
        self.assertIn(f"threads{n_cores}", cfg.crab.JobType.psetName)

    def test_a_value_that_cannot_be_megabytes_is_refused(self):
        with self.assertRaises(ValueError) as caught:
            resources({"max_cores": 4}, n_cpus=4, memory=10)
        self.assertIn("in MB", str(caught.exception))

    def test_zero_and_minus_one_both_mean_unset(self):
        for unset in (0, -1):
            self.assertEqual(
                resources({"max_cores": 4}, n_cpus=4, memory=unset, crab_memory=unset)[
                    :2
                ],
                (4, 10000),
            )


class TheRemovedKeys(unittest.TestCase):
    """`max_memory_mb` used to seed a formula that overwrote it, so its value was never the
    request. Read as a request now, the 2500 sitting in the live config would pin RunProd to one
    core -- the configuration whose measured memory-kill rate was 1.8-3.8 %."""

    def _cfg_with(self, key):
        from dsprod.crab import CrabWorkflow as CW

        task = mock.Mock()
        task._crab_cfg = CW._crab_cfg.__get__(task)
        with mock.patch("dsprod.config.get_global", return_value={"crab": {key: 2500}}):
            return task._crab_cfg()

    def test_the_legacy_keys_fail_closed(self):
        for key in ("max_memory_mb", "max_memory_mb_per_core"):
            with self.assertRaises(RuntimeError) as caught:
                self._cfg_with(key)
            self.assertIn(key, str(caught.exception))
            self.assertIn("memory", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
