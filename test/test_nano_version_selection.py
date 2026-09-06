#!/usr/bin/env python3
"""Choosing which NanoAOD versions an era produces.

Every version is a full second copy of the era's events, so producing one instead of two is the
difference between ~1.1 TB and ~0.5 TB for the 2022/2023 eras of the full 40-mass grid. The setup
says what an era *can* produce; `--nano-versions` narrows that at run time so a cheaper pass needs
no second setup file.

The real `Task.era_nano_versions` is exercised here against a minimal stand-in carrying only the
three attributes it reads -- constructing a law Task would need a whole setup on disk, and a
hand-written copy of the method would drift from it.
"""

import os
import sys
import types
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

from dsprod.tasks import Task


def resolve(era, setup_versions, requested=()):
    """Call the real resolver with only what it reads."""
    stub = types.SimpleNamespace(
        prod_setup={"nano_versions": setup_versions},
        nano_versions=tuple(requested),
        setup="a_setup.yaml",
    )
    return Task.era_nano_versions(stub, era)


class TestSetupResolution(unittest.TestCase):
    """What the setup alone says, unchanged by this feature."""

    def test_per_era_entry(self):
        versions = {"Run3_2023": ["v12", "v15"], "Run3_2024": ["v15"]}
        self.assertEqual(resolve("Run3_2023", versions), ["v12", "v15"])
        self.assertEqual(resolve("Run3_2024", versions), ["v15"])

    def test_default_key_is_the_fallback(self):
        versions = {"default": ["v12"], "Run3_2024": ["v15"]}
        self.assertEqual(resolve("Run3_2022", versions), ["v12"])
        self.assertEqual(resolve("Run3_2024", versions), ["v15"])

    def test_a_plain_list_applies_to_every_era(self):
        self.assertEqual(resolve("Run3_2022", ["v12", "v15"]), ["v12", "v15"])

    def test_an_era_with_no_entry_and_no_default_produces_nothing(self):
        self.assertEqual(resolve("Run3_2022", {"Run3_2024": ["v15"]}), [])


class TestRunTimeOverride(unittest.TestCase):
    """`--nano-versions` narrows the setup, and never widens it."""

    SETUP = {
        "Run3_2023": ["v12", "v15"],
        "Run3_2023BPix": ["v12", "v15"],
        "Run3_2024": ["v15"],
    }

    def test_dropping_v15_halves_what_an_era_writes(self):
        self.assertEqual(resolve("Run3_2023", self.SETUP, ["v12"]), ["v12"])
        self.assertEqual(resolve("Run3_2023BPix", self.SETUP, ["v12"]), ["v12"])

    def test_selecting_both_is_the_setup(self):
        self.assertEqual(
            resolve("Run3_2023", self.SETUP, ["v12", "v15"]), ["v12", "v15"]
        )

    def test_setup_order_is_kept_whatever_the_option_order(self):
        self.assertEqual(
            resolve("Run3_2023", self.SETUP, ["v15", "v12"]), ["v12", "v15"]
        )

    def test_it_cannot_widen_an_era(self):
        # Run3_2024 has no v12: asking for one must not invent it
        with self.assertRaises(RuntimeError) as cm:
            resolve("Run3_2024", self.SETUP, ["v12"])
        self.assertIn("Run3_2024", str(cm.exception))
        self.assertIn("v15", str(cm.exception))

    def test_emptying_an_era_is_refused_rather_than_silent(self):
        # the same rule as --eras/--points: a run that produces nothing must not start
        with self.assertRaises(RuntimeError) as cm:
            resolve("Run3_2023", self.SETUP, ["v99"])
        msg = str(cm.exception)
        self.assertIn("--nano-versions", msg)
        self.assertIn("v99", msg)
        self.assertIn("a_setup.yaml", msg)

    def test_no_override_leaves_every_era_alone(self):
        for era, expected in self.SETUP.items():
            self.assertEqual(resolve(era, self.SETUP), expected)


if __name__ == "__main__":
    unittest.main()
