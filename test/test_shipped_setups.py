#!/usr/bin/env python3
"""Every production setup this repository ships is loaded and checked here.

The suite exercised one setup, `models/X_HH/setups/Run3_XHHbbWW.yaml`, because it was the only
one. A second setup can name a final state whose fragment does not exist, a production mode with
no cards, an era it never declares, or samples that do not fill whole merged files -- and nothing
would say so until a production run reached the point. Loading a setup is cheap and checks most of
that on its own (`Task.__init__` resolves the process plugin, the conditions file and the merge
granularity), so the setups are walked rather than listed: a setup added later is covered without
touching this file.

The one failure mode none of that catches is a point whose NAME disagrees with its own parameters.
Nothing downstream reads the name, so a point called `..._M-9999` carrying `mass: 250` produces a
perfectly good sample under a name no analysis can match it by, and it would be found by a person,
late. The convention belongs to the model rather than to this walk, so the check is the plugin's
`validate(point)` hook (a no-op in the base class) and what is exercised here is that it is called
for every point of every setup.

What is NOT checked is reported when the module finishes rather than passed over in silence: the
gridpack payloads, which the store never holds locally (it is sparse, and the CI runner does not
check it out at all -- see .github/workflows/unit-tests.yaml).
"""

import glob
import os
import sys
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

# the process plugins live under `<ANALYSIS_PATH>/models`, and the checkout is the area
os.environ["ANALYSIS_PATH"] = dsprod_repo

import luigi  # noqa: E402
import yaml  # noqa: E402

from dsprod import gridpack_store  # noqa: E402
from dsprod.tasks import CollectGridpacks, Task  # noqa: E402

SETUPS = sorted(glob.glob(os.path.join(dsprod_repo, "models", "*", "setups", "*.yaml")))

#: task families a setup's `resources:` block writes into luigi's configuration
RESOURCE_FAMILIES = ("RunProd", "MakeGridpack", "NanoMergeTask")

#: filled by the walk, reported by `tearDownModule`
_not_checked = []


def clear_setup_cache():
    """Empty `Task`'s process-wide setup cache, which refuses a second path once loaded."""
    Task.setup_path = None
    Task.prod_setup = None
    Task.conditions = None
    Task.process = None
    Task.all_points = None


def snapshot_resources():
    """The `resources:` sections currently in luigi's configuration."""
    cfg = luigi.configuration.get_config()
    return {
        family: dict(cfg.items(family)) if cfg.has_section(family) else None
        for family in RESOURCE_FAMILIES
    }


def restore_resources(before):
    """Put luigi's configuration back.

    Loading a setup writes its `resources:` into the process-wide luigi config and nothing takes
    them out again -- one law process runs one production. A test module that reads every shipped
    setup would otherwise hand the last one's numbers to whatever runs next, which is a real
    difference as soon as two setups ask for different resources.
    """
    cfg = luigi.configuration.get_config()
    for family, items in before.items():
        cfg.remove_section(family)
        if items is not None:
            cfg.add_section(family)
            for name, value in items.items():
                cfg.set(family, name, value)


class TestShippedSetups(unittest.TestCase):
    def setUp(self):
        clear_setup_cache()
        self.addCleanup(clear_setup_cache)
        self.addCleanup(restore_resources, snapshot_resources())

    def test_at_least_one_setup_is_shipped(self):
        """Guards the walk itself: a glob that matches nothing would pass every test below."""
        self.assertTrue(SETUPS, "no setup found under models/*/setups/*.yaml")

    def test_every_setup_loads_and_describes_real_points(self):
        store = gridpack_store.store_root(dsprod_repo)
        have_store = gridpack_store.is_available(store)
        # one listing for every question below: a `contains()` per point spawns a `git ls-tree`
        # each and costs more than the whole suite
        tracked = gridpack_store.tracked_paths(store)
        if not have_store:
            _not_checked.append(
                "which gridpacks the store tracks -- `gridpacks/` is not checked out here"
            )

        for path in SETUPS:
            rel = os.path.relpath(path, dsprod_repo)
            with self.subTest(setup=rel):
                clear_setup_cache()
                # the real load: resolves `process:`, reads `conditions:`, enumerates the points
                # and refuses samples that would not fill whole merged files
                task = CollectGridpacks(setup=path)
                # luigi caches task instances by their parameters and skips `__init__` on a hit,
                # so a construction that returns a cached instance would leave the cache cleared
                # above empty and every check below vacuous
                self.assertEqual(
                    Task.setup_path,
                    path,
                    f"{rel}: the task was not constructed from this setup (luigi returned a "
                    "cached instance), so nothing below would have been checked",
                )
                points = Task.all_points
                self.assertTrue(points, f"{rel}: setup enumerates no point")

                names = [p.name for p in points]
                dupes = sorted({n for n in names if names.count(n) > 1})
                self.assertFalse(dupes, f"{rel}: duplicate point names {dupes}")

                eras = set(Task.prod_setup["eras"])
                with open(path) as f:
                    raw = yaml.safe_load(f)
                for p in raw["points"]:
                    unknown = sorted(set(p["events_total"]) - eras)
                    self.assertFalse(
                        unknown,
                        f"{rel}: point {p['name']} sets events_total for {unknown}, "
                        f"which the setup does not declare in `eras:`",
                    )

                for point in points:
                    # whatever the model considers a valid point -- for X_HH, that its name and
                    # its parameters agree
                    try:
                        task.process.validate(point)
                    except Exception as e:
                        self.fail(f"{rel}: {type(e).__name__}: {e}")

                    fragment = task.process.gen_fragment(point)
                    self.assertTrue(
                        os.path.isfile(fragment),
                        f"{rel}: point {point.name} asks for the gen fragment {fragment}, "
                        "which does not exist",
                    )
                    cards = task.process.gridpack(point).cards_template
                    for card in (
                        "proc_card",
                        "run_card",
                        "customizecards",
                        "extramodels",
                    ):
                        self.assertTrue(
                            os.path.isfile(os.path.join(cards, f"{card}.dat")),
                            f"{rel}: point {point.name} has no {card}.dat under {cards}",
                        )

                    if have_store:
                        # tracked, not present: the checkout is sparse, so this says the store
                        # knows the gridpack, which is what "nothing here needs MakeGridpack"
                        # rests on
                        gridpack = task.process.gridpack_rel_path(point)
                        self.assertIn(
                            gridpack,
                            tracked,
                            f"{rel}: point {point.name} would have to generate its gridpack -- "
                            f"the store does not track {gridpack}",
                        )


def tearDownModule():
    for item in ["the gridpack payloads (the store checkout is sparse)"] + _not_checked:
        print(f"NOT CHECKED by {os.path.basename(__file__)}: {item}", file=sys.stderr)


if __name__ == "__main__":
    unittest.main()
