#!/usr/bin/env python3
"""Deleting a `produced/` record whose nano file is gone -- and nothing else.

A record says a seed's nano file exists, and the merge trusts it: that trust is what lets a merge
delete the files it consumed without the next run reading a finished era as unproduced. When a
staged file disappears anyway, the record outlives it and every attempt at that merge group fails
with "N of 50 staged nano files of this merge group are gone" (three groups of the Run3_2022EE
production, 2026-09-15, each missing exactly one file of fifty).

The repair is to let those seeds run again, i.e. to delete their records. What these tests hold
down is the blast radius of getting that wrong: after a merge, EVERY record of the group has no
staged file and is perfectly healthy, so a prune that does not understand merged files would delete
an era's worth of records and re-produce it -- weeks of grid time for a listing that blinked.
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

law.contrib.load("cms")

from dsprod.tasks import PruneProducedRecords  # noqa: E402

ERA = "Run3_2022EE"
VERSION = "v12"
#: ten seeds in two merge groups of five, the shape of the real thing at 1/10 scale
SEEDS = 10
PER_MERGE = 5


class Dir:
    """A remote directory with the semantics DSProd's gfal interface really has.

    Two of them matter here and both are the opposite of the obvious guess. `listdir` RAISES when
    the listing fails -- and also when the directory is not there, since `gfal-ls` of an absent
    path fails like any other error. And `exists()` cannot tell those apart at all: the interface
    answers it by listing the parent with `silent=True`, so a failed `gfal-ls` comes back as
    "absent", is cached, and the ancestors are marked absent with it. A fixture that raised from
    `exists()` is what let the first version of this task look safe.
    """

    def __init__(self, names=(), exists=True, error=None, basename="P", parent=None):
        self.names = set(names)
        self._exists = exists
        self.error = error
        self.basename = basename
        self.parent = parent

    def listdir(self):
        if self.error:
            raise OSError(self.error)
        if not self._exists:
            raise OSError(f"gfal-ls: {self.basename}: No such file or directory")
        return sorted(self.names)

    def exists(self):
        # what the real one does: a blink and an absence are the same answer
        return self._exists and not self.error

    def vanish(self):
        """Not there at all -- gone from storage and from its parent's listing."""
        self._exists = False
        self.parent.names.discard(self.basename)

    def vanish_with_parent(self):
        """Neither this directory nor the one above it exists: a production before its first
        merge, where the whole `nanoAOD_<version>/<era>` branch has yet to be created.
        """
        self.vanish()
        self.parent.vanish()


def tree(basename, names=(), siblings=("other",)):
    """A directory with the two ancestors that can say whether it is there.

    Two levels, because that is the shape of a young production: neither the point's directory nor
    the era's above it exists yet, and the first thing that can answer is the one above those.
    """
    grandparent = Dir(names={"era"}, basename="nanoAOD_v12")
    parent = Dir(names=set(siblings) | {basename}, basename="era", parent=grandparent)
    return Dir(names=names, basename=basename, parent=parent)


class Target:
    def __init__(self, directory, name, fail=False):
        self.parent = directory
        self.name = name
        self.fail = fail

    def remove(self, silent=True, **kwargs):
        # law's `FileSystemTarget.remove` defaults to silent=True and the gfal interface then
        # swallows the error and returns False -- a removal that did not happen
        if self.fail:
            if not silent:
                raise OSError("gfal: failed to remove")
            return  # law's FileSystemTarget.remove returns None either way
        self.parent.names.discard(self.name)


def task(records=(), staged=(), merged=(), remove_fails=False, **attrs):
    """`PruneProducedRecords` over an in-memory storage of one (era, point, version)."""
    record_dir = tree("produced", [f"nano_{VERSION}_{s}.json" for s in records])
    staged_dir = tree("staging", [f"nano_{VERSION}_{s}.root" for s in staged])
    merged_dir = tree("merged", [f"nano_{VERSION}_{g}.root" for g in merged])

    point = mock.Mock()
    point.n_jobs.return_value = attrs.pop("n_seeds", SEEDS)

    t = mock.Mock()
    t.prod_points = [point]
    t.prod_setup = {"files_per_merge": PER_MERGE}
    t.process.point_name.return_value = "P"
    t.produced_nano_target = lambda e, p, v, s: Target(
        record_dir, f"nano_{v}_{s}.json", fail=remove_fails
    )
    t.staged_nano_target = lambda e, p, v, s: Target(staged_dir, f"nano_{v}_{s}.root")
    t.merged_nano_target = lambda e, p, v, g: Target(merged_dir, f"nano_{v}_{g}.root")
    # the real listing helper, so its strictness is what is under test
    t._names = PruneProducedRecords._names
    t.prune = False
    t.max_stale_fraction = 0.5
    t.remove_threads = 4
    for k, v in attrs.items():
        setattr(t, k, v)
    t.dirs = (record_dir, staged_dir, merged_dir)
    return t


def run(t):
    """One (era, point, version) unit. `run()` itself only fans these out over threads."""
    with mock.patch("builtins.print") as printed:
        PruneProducedRecords._check_and_prune(t, ERA, 0, VERSION)
    return " ".join(str(c.args[0]) for c in printed.call_args_list)


def remaining(t):
    return sorted(int(n.split("_")[-1].split(".")[0]) for n in t.dirs[0].names)


class AHealthyProductionLosesNothing(unittest.TestCase):
    def test_after_a_merge_every_record_is_kept(self):
        """The normal state, and the dangerous one: the merge deleted all the staged files it
        consumed, so every record of a merged group has none. None of them is stale."""
        t = task(records=range(1, 11), staged=(), merged=(0, 1), prune=True)
        out = run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))
        self.assertIn("nothing stale", out)

    def test_before_a_merge_every_record_is_kept(self):
        t = task(records=range(1, 11), staged=range(1, 11), merged=(), prune=True)
        run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))

    def test_a_half_merged_point_keeps_both_halves(self):
        """One group merged and dropped, the other still staged -- the state a production is in
        for most of its life."""
        t = task(records=range(1, 11), staged=range(6, 11), merged=(0,), prune=True)
        run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))

    def test_a_seed_with_no_record_is_not_invented(self):
        t = task(records=(1, 2), staged=(1, 2), merged=(), prune=True)
        run(t)
        self.assertEqual(remaining(t), [1, 2])


class ALostFileCostsItsRecord(unittest.TestCase):
    def test_the_stale_record_is_found(self):
        t = task(records=range(1, 11), staged=[s for s in range(1, 11) if s != 7])
        out = run(t)
        self.assertIn("1 of 10 records are stale", out)
        self.assertIn("seeds 7", out)

    def test_nothing_is_deleted_without_prune(self):
        """The cost of a wrong deletion is a seed produced again from scratch."""
        t = task(records=range(1, 11), staged=[s for s in range(1, 11) if s != 7])
        out = run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))
        self.assertIn("--prune", out)

    def test_with_prune_exactly_that_record_goes(self):
        t = task(
            records=range(1, 11), staged=[s for s in range(1, 11) if s != 7], prune=True
        )
        out = run(t)
        self.assertEqual(remaining(t), [1, 2, 3, 4, 5, 6, 8, 9, 10])
        self.assertIn("1 stale record(s) deleted", out)

    def test_the_incident_of_2026_09_15(self):
        """One staged file of fifty gone, the group not merged: exactly one record to delete."""
        t = task(
            records=range(1, 51),
            staged=[s for s in range(1, 51) if s != 45],
            merged=(),
            n_seeds=50,
            prune=True,
        )
        run(t)
        self.assertNotIn(45, remaining(t))
        self.assertEqual(len(remaining(t)), 49)


class AndTheBlastRadiusIsHeldDown(unittest.TestCase):
    def test_a_listing_that_fails_deletes_nothing(self):
        """The failure that would cost an era: a staging listing that raises must not be read as
        "nothing is staged", which would make every record of the point look stale."""
        t = task(records=range(1, 11), staged=range(1, 11), prune=True)
        t.dirs[1].error = "gfal: connection timed out"
        with self.assertRaises(OSError):
            run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))

    def test_a_merged_listing_that_fails_deletes_nothing(self):
        t = task(records=range(1, 11), staged=(), merged=(0, 1), prune=True)
        t.dirs[2].error = "gfal: connection timed out"
        with self.assertRaises(OSError):
            run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))

    def test_too_many_stale_records_is_read_as_a_fault_not_as_loss(self):
        """Half a point missing is not "files were lost", it is the wrong production or an
        unreadable staging area -- so it stops and says what to check."""
        t = task(records=range(1, 11), staged=(1, 2, 3), merged=(), prune=True)
        with self.assertRaises(RuntimeError) as caught:
            run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))
        message = str(caught.exception)
        self.assertIn("70%", message)
        self.assertIn("max-stale-fraction", message)

    def test_the_threshold_can_be_raised_deliberately(self):
        t = task(
            records=range(1, 11),
            staged=(1, 2, 3),
            merged=(),
            prune=True,
            max_stale_fraction=1.0,
        )
        run(t)
        self.assertEqual(remaining(t), [1, 2, 3])

    def test_a_directory_that_does_not_exist_yet_is_empty_not_an_error(self):
        """A point that has never been merged has no merged directory at all -- and `gfal-ls` of
        it fails exactly like an unreadable one, so absence is established from the parent.
        """
        t = task(records=range(1, 11), staged=range(1, 11), merged=(), prune=True)
        t.dirs[2].vanish()
        run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))

    def test_a_blinked_listing_is_not_read_as_an_empty_one(self):
        """The failure that would cost an era, in the shape it really takes: `gfal-ls` of the
        staging tree fails while the parent still lists it, so the directory is there and unread.

        `exists()` answers this case False -- it is implemented by listing the parent silently --
        which is why nothing here may ask it.
        """
        t = task(records=range(1, 11), staged=range(1, 11), merged=(0,), prune=True)
        t.dirs[1].error = "gfal: transfer timed out"
        with self.assertRaises(OSError):
            run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))

    def test_an_absent_directory_whose_parent_cannot_be_read_stops_the_prune(self):
        """Absence counts only when something could actually say so."""
        t = task(records=range(1, 11), staged=range(1, 11), merged=(), prune=True)
        t.dirs[2].vanish()
        t.dirs[2].parent.error = "gfal: connection timed out"
        with self.assertRaises(OSError):
            run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))

    def test_an_era_before_its_first_merge_can_still_be_pruned(self):
        """Nothing is merged anywhere yet, so the whole merged branch of the tree is missing --
        which must read as "nothing is merged", not as "the storage is unreadable"."""
        t = task(
            records=range(1, 11),
            staged=[s for s in range(1, 11) if s != 7],
            merged=(),
            prune=True,
        )
        t.dirs[2].vanish_with_parent()
        run(t)
        self.assertEqual(remaining(t), [1, 2, 3, 4, 5, 6, 8, 9, 10])

    def test_but_not_when_the_climb_runs_out_of_answers(self):
        t = task(records=range(1, 11), staged=range(1, 11), merged=(), prune=True)
        t.dirs[2].vanish_with_parent()
        t.dirs[2].parent.parent.error = "gfal: connection timed out"
        with self.assertRaises(OSError):
            run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))

    def test_a_merge_that_completes_mid_prune_costs_nothing(self):
        """The reason the staging tree is listed before the merged one.

        A merge uploads its merged file and only then deletes the staged inputs, so a group that
        merges between the two listings is seen by whichever runs second. Staged first: the seeds
        are still staged when it looks. Merged first: the merged file is not there yet when it
        looks, the staged files are gone by the time it looks at them, and 50 healthy records are
        deleted.
        """
        t = task(records=range(1, 11), staged=range(1, 11), merged=(), prune=True)
        staged_dir, merged_dir = t.dirs[1], t.dirs[2]
        state = {"listings": 0}
        original = staged_dir.listdir
        merged_original = merged_dir.listdir

        def merge_after_the_first_listing():
            """Whichever directory is listed first, group 0 finishes merging right after it --
            so the second listing sees the world the first one did not."""
            if state["listings"] == 0:
                merged_dir.names.add(f"nano_{VERSION}_0.root")
                staged_dir.names -= {f"nano_{VERSION}_{s}.root" for s in range(1, 6)}
            state["listings"] += 1

        def staged_listdir():
            names = original()
            merge_after_the_first_listing()
            return names

        def merged_listdir():
            names = merged_original()
            merge_after_the_first_listing()
            return names

        staged_dir.listdir = staged_listdir
        merged_dir.listdir = merged_listdir
        run(t)
        self.assertEqual(remaining(t), list(range(1, 11)))

    def test_a_removal_that_fails_is_not_reported_as_done(self):
        """An operator repairing a production mid-incident must not be told the records are gone
        when the storage refused: the merge would be relaunched into the same failure.
        """
        t = task(
            records=range(1, 11),
            staged=[s for s in range(1, 11) if s != 7],
            prune=True,
            remove_fails=True,
        )
        with self.assertRaises(OSError):
            run(t)

    def test_exactly_at_the_threshold_is_still_pruned(self):
        """The rail is a refusal above the share, not at it."""
        t = task(records=range(1, 11), staged=range(1, 6), merged=(), prune=True)
        run(t)
        self.assertEqual(remaining(t), [1, 2, 3, 4, 5])

    def test_a_record_for_a_seed_the_era_does_not_have_is_left_alone(self):
        """`events_total` shrank, or the record belongs to another production entirely: either
        way this task is not the one to decide that."""
        t = task(records=range(1, 16), staged=range(1, 11), merged=(), prune=True)
        run(t)
        self.assertEqual(remaining(t), list(range(1, 16)))


class ItNeverReportsItselfDone(unittest.TestCase):
    """Records go stale again tomorrow, so the check must repeat -- and it must also terminate.

    It is a plain task, not a workflow, for exactly that reason: law's local workflow yields its
    branches as dynamic dependencies and luigi re-runs the workflow once they finish, re-checking
    each branch, so a never-complete branch is rescheduled on every pass. A live production hit it
    (2026-09-15) and ran wave after wave; an in-memory "already checked" flag does not help either,
    since luigi runs each branch in its own process.
    """

    def task(self):
        setup = os.path.join(dsprod_repo, "models/X_HH/setups/Run3_XHHbbWW.yaml")
        if not os.path.exists(setup):
            self.skipTest("models submodule not checked out")
        return PruneProducedRecords(
            setup=setup, eras=("Run3_2022EE",), points=("*_M-250",)
        )

    def test_it_is_not_a_workflow(self):
        self.assertNotIsInstance(self.task(), law.LocalWorkflow)
        self.assertFalse(hasattr(self.task(), "branch_map"))

    def test_it_always_reports_itself_incomplete(self):
        self.assertFalse(self.task().complete())

    def test_luigi_marks_it_done_anyway_so_a_run_terminates(self):
        """The property the loop depended on: luigi only re-checks `complete()` after `run()` when
        `check_complete_on_run` is set, and it is not."""
        from luigi.worker import worker as luigi_worker

        self.assertFalse(luigi_worker().check_complete_on_run)


if __name__ == "__main__":
    unittest.main()
