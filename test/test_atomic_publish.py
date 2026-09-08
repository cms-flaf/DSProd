#!/usr/bin/env python3
"""How a finished artefact becomes visible at its final path.

Every remote write DSProd makes builds a complete file locally and then publishes it; nothing
appends, and no reader watches a product grow. Two of those writes cannot be recovered if they are
half-published: the merged nano, whose 50 staged inputs are deleted immediately afterwards, and the
`produced/` record that is the production's completeness signal.

`copy_flag` -- the mode used until now -- copies straight onto the final path, so the final name
exists while its content is partial, and it removes the previous file first, so a copy that dies
leaves nothing behind. `copy_rename` uploads to a tmp name, verifies the checksum and renames onto
the target, which on this storage is atomic. Three things have to hold together for that to be an
improvement, and each of them is a separate test below: the target must not be removed up front,
the publish must happen via the rename, and the tmp name must be unique per writer -- a shared tmp
path merely moves the collision, which matters because the watchdog deliberately creates a second
writer for every job it gives up on.
"""

import os
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod import grid_tools  # noqa: E402

TARGET = "root://example.cern.ch//store/x/nano_v12_1.root"


class Recorder:
    """Stands in for the gfal CLI, recording what would have been done to the storage."""

    def __init__(self, existing=(TARGET,)):
        self.existing = set(existing)
        self.removed = []
        self.copied = []
        self.renamed = []

    def install(self, stack):
        p = lambda n, f: stack.enter_context(
            mock.patch.object(grid_tools, n, f)
        )  # noqa: E731
        p("gfal_exists", lambda path, **kw: path in self.existing)
        p("gfal_rm", self._rm)
        p("gfal_copy", self._copy)
        p("gfal_rename", self._rename)
        p("gfal_stat", lambda path, **kw: {"type": "regular file"})
        p("gfal_sum", lambda path, **kw: 0xABCD)
        return self

    def _rm(self, path, **kw):
        self.removed.append(path)
        self.existing.discard(path)

    def _copy(self, src, dst, **kw):
        self.copied.append((src, dst))
        self.existing.add(dst)

    def _rename(self, src, dst, **kw):
        self.renamed.append((src, dst))
        self.existing.discard(src)
        self.existing.add(dst)


def publish(mode, existing=(TARGET,)):
    import contextlib

    with contextlib.ExitStack() as stack:
        rec = Recorder(existing).install(stack)
        grid_tools.gfal_copy_safe(
            "/local/nano.root", TARGET, copy_mode=mode, voms_token="tok", verbose=0
        )
    return rec


class PublishingByRename(unittest.TestCase):
    def test_the_published_file_is_never_taken_away_first(self):
        """The regression: `copy_rename` used to delete the target before uploading, so the
        product was absent for the whole duration of the copy -- indefinitely, under a job that
        then froze."""
        rec = publish("copy_rename")
        self.assertNotIn(TARGET, rec.removed)

    def test_the_target_is_only_ever_created_by_the_rename(self):
        rec = publish("copy_rename")
        self.assertEqual([dst for _, dst in rec.renamed], [TARGET])
        self.assertNotIn(TARGET, [dst for _, dst in rec.copied])

    def test_the_upload_goes_to_a_marked_tmp_name_after_the_extension(self):
        """A marker before `.root` would be picked up by anything globbing `*.root`."""
        rec = publish("copy_rename")
        ((_, tmp),) = rec.copied
        self.assertTrue(tmp.startswith(TARGET), tmp)
        self.assertTrue(grid_tools.is_copy_rename_tmp(tmp), tmp)
        self.assertFalse(grid_tools.is_copy_rename_tmp(TARGET))

    def test_two_writers_of_the_same_target_do_not_share_a_tmp_path(self):
        """A shared tmp path only moves the collision: `download()` removes the tmp at the top of
        every attempt, so one writer would delete the other's upload."""
        first, second = publish("copy_rename"), publish("copy_rename")
        self.assertNotEqual(first.copied[0][1], second.copied[0][1])

    def test_the_checksum_is_verified_before_the_rename_not_after(self):
        rec = publish("copy_rename")
        self.assertEqual(len(rec.renamed), 1, "the rename must be the last step")

    def test_copy_flag_still_behaves_as_it_did(self):
        """The other mode is unchanged: it copies onto the target and clears it first."""
        rec = publish("copy_flag")
        self.assertIn(TARGET, rec.removed)
        self.assertIn(TARGET, [dst for _, dst in rec.copied])
        self.assertEqual(rec.renamed, [])

    def test_a_first_publish_needs_no_removal_either(self):
        rec = publish("copy_rename", existing=())
        self.assertEqual(rec.removed, [])
        self.assertEqual([dst for _, dst in rec.renamed], [TARGET])


class WhatLawUses(unittest.TestCase):
    def test_every_local_to_remote_write_publishes_by_rename(self):
        """The chokepoint: all seven of DSProd's remote writes go through this one call."""
        import inspect

        from dsprod import law_gfal

        src = inspect.getsource(law_gfal.GFALFileInterface.filecopy)
        head = src[: src.index("elif dst_local")]
        self.assertIn('copy_mode="copy_rename"', head)


if __name__ == "__main__":
    unittest.main()
