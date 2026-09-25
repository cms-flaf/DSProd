#!/usr/bin/env python3
""" "Could not list it" is not "it is not there".

`GFALFileInterface.exists()` answers by listing the parent directory, and the listing used to
return an empty list for *any* failure -- a timeout, an SSL handshake, an endpoint under load --
which `exists()` then reported as "the file does not exist", cached as a negative, and propagated
up the tree by marking the ancestors absent.

Every completeness decision in DSProd is built on that answer, so one blinked listing is a wrong
decision about a product. On 2026-09-18 it was both halves of one incident: 1400 jobs listing the
same `premix/` directory at once each read the one blink as "the premix list is gone", rebuilt it
on the worker and died on the DAS guard; and with `crab_check_job_completeness()` on, the same
blink would demote a job that really did finish. A listing that fails is now retried and then
raised; only gfal's own "no such file or directory" means absent.
"""

import os
import sys
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from dsprod import grid_tools, law_gfal  # noqa: E402
from dsprod.grid_tools import GfalError, gfal_ls_checked, is_absent_error  # noqa: E402


class Entry:
    def __init__(self, name):
        self.name = name


# the real messages, captured against the endpoints this production uses: xrootd at FNAL
# (`fs_default`) and davs on CERNBox (`fs_watchdog`)
ABSENT = GfalError(
    'gfal_ls: unable to list "root://cmseos.fnal.gov//eos/uscms/store/x"\nError code: 1 '
    "gfal-ls error: 2 (No such file or directory) - Failed to stat file "
    "(No such file or directory)"
)
ABSENT_DAVS = GfalError(
    'gfal_ls: unable to list "davs://eoshome-k.cern.ch:8444/eos/user/k/x"\nError code: 1 '
    "gfal-ls error: 2 (No such file or directory) - Result HTTP 404 : File not found "
    "after 1 attempts"
)
NO_CREDENTIAL = GfalError(
    'gfal_ls: unable to list "root://cmseos.fnal.gov//eos/uscms/store/x"\nError code: 1 '
    "TLS: Unable to use cert+key file /tmp/x509up_u0; does not exist. "
    "gfal-ls error: 52 (Invalid exchange)"
)
UNREACHABLE = GfalError(
    'gfal_ls: unable to list "davs://host/x"\nError code: 1 '
    "gfal-ls error: 13 (Permission denied) - (Neon): SSL handshake failed: "
    "Connection timed out"
)


class Classification(unittest.TestCase):
    def test_gfal_says_which_one_it_is(self):
        self.assertTrue(
            is_absent_error(ABSENT), "xrootd at FNAL, the products' endpoint"
        )
        self.assertTrue(is_absent_error(ABSENT_DAVS), "davs on CERNBox, the watchdog's")
        self.assertFalse(is_absent_error(UNREACHABLE))

    def test_a_missing_credential_is_not_an_absent_file(self):
        # the failure this must never call "absent": with no usable proxy every path in the
        # production would read as gone, and a completeness check would condemn the whole sample
        self.assertFalse(is_absent_error(NO_CREDENTIAL))

    def test_an_absent_path_is_reported_at_once(self):
        with mock.patch.object(grid_tools, "gfal_ls", side_effect=ABSENT) as ls:
            self.assertIsNone(gfal_ls_checked("davs://host/x", voms_token="t"))
        self.assertEqual(ls.call_count, 1, "an absent path is not worth retrying")

    def test_a_blink_is_retried_and_the_listing_returned(self):
        ls = mock.Mock(side_effect=[UNREACHABLE, [Entry("a.root")]])
        with mock.patch.object(grid_tools, "gfal_ls", ls):
            entries = gfal_ls_checked("davs://host/x", voms_token="t", delay=0)
        self.assertEqual([e.name for e in entries], ["a.root"])
        self.assertEqual(ls.call_count, 2)

    def test_an_endpoint_that_stays_down_raises(self):
        ls = mock.Mock(side_effect=UNREACHABLE)
        with mock.patch.object(grid_tools, "gfal_ls", ls):
            with self.assertRaises(GfalError):
                gfal_ls_checked("davs://host/x", voms_token="t", attempts=3, delay=0)
        self.assertEqual(
            ls.call_count, 3, "every attempt must be spent before giving up"
        )


class TheInterface(unittest.TestCase):
    def setUp(self):
        with mock.patch.object(
            law_gfal, "get_voms_proxy_info", return_value={"path": "/tmp/token"}
        ):
            self.fs = law_gfal.GFALFileInterface(base="davs://host/base")

    def listing(self, result):
        """Patch the one call `listdir` makes; `result` is a return value or an exception."""
        kwargs = (
            {"side_effect": result}
            if isinstance(result, Exception)
            else {"return_value": result}
        )
        return mock.patch.object(law_gfal, "gfal_ls_checked", **kwargs)

    def test_an_absent_directory_still_lists_as_empty_when_silent(self):
        with self.listing(None):
            self.assertEqual(self.fs.listdir("some/dir", silent=True), [])

    def test_an_absent_directory_still_raises_when_not_silent(self):
        with self.listing(None):
            with self.assertRaises(GfalError):
                self.fs.listdir("some/dir", silent=False)

    def test_a_failed_listing_raises_even_when_silent(self):
        # the regression: this used to return [] and be read as "the directory is empty"
        with self.listing(UNREACHABLE):
            with self.assertRaises(GfalError):
                self.fs.listdir("some/dir", silent=True)

    def test_exists_does_not_answer_from_a_failed_listing(self):
        # the incident itself: `exists()` said False for a file that was there all along
        with self.listing(UNREACHABLE):
            with self.assertRaises(GfalError):
                self.fs.exists("some/dir/file.txt")

    def test_a_failed_listing_poisons_no_cache_entry(self):
        with self.listing(UNREACHABLE):
            with self.assertRaises(GfalError):
                self.fs.exists("some/dir/file.txt")
        for path in ("some/dir/file.txt", "some/dir", "some"):
            cached, _ = self.fs.path_cache.get(self.fs.uri(path))
            self.assertIsNone(
                cached,
                f"{path} was cached as absent because one listing failed",
            )

    def test_an_absence_is_still_cached(self):
        with self.listing(None):
            self.assertFalse(self.fs.exists("some/dir/file.txt"))
        cached, _ = self.fs.path_cache.get(self.fs.uri("some/dir/file.txt"))
        self.assertIs(cached, False, "a real absence must stay cheap to re-ask")


if __name__ == "__main__":
    unittest.main()
