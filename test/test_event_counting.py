#!/usr/bin/env python3
"""Counting the entries of a merge group: once for the whole group, and never off stdout.

Each call to `run_step.count_events` enters the nano release (`scram runtime`, in a container when
the worker OS differs), which dwarfs the counting itself. Called per file, a group of 50 inputs
plus its merged output spent about 10 minutes of its 3 h slot on nothing but that.

The result cannot be read off stdout either: ROOT prints its own warnings there, so the last line
of the output is whatever ROOT said last, not a count. The child therefore reports through a JSON
file keyed by path.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

from dsprod import run_step  # noqa: E402

#: what ROOT puts on stdout around a count, and what a positional read would return
ROOT_NOISE = [
    "Warning in <TClass::Init>: no dictionary for class edm::Hash<1> is available",
    "Warning in <TFile::Init>: file probably not closed, trying to recover",
]

#: `step_params` reaches `_cmsenv_prefix` only, which is replaced here
PARAMS = {"CMSSW": "CMSSW_15_0_0", "SCRAM_ARCH": "el9_amd64_gcc12"}


class CountCase(unittest.TestCase):
    """Runs the real `count_events` with only the CMSSW invocation replaced."""

    def setUp(self):
        self.work_dir = tempfile.mkdtemp(prefix="dsprod_count_test_")
        self.addCleanup(shutil.rmtree, self.work_dir, True)
        self.calls = []
        self.requests = []
        prefix = mock.patch(
            "dsprod.run_step._cmsenv_prefix", return_value="cmsenv-would-run"
        )
        prefix.start()
        self.addCleanup(prefix.stop)

    def child(self, counts=None, stdout_lines=(), write=True):
        """Stand in for the CMSSW invocation, reading the request the way the real child does."""

        def ps_call(cmd, **kwargs):
            self.calls.append(cmd)
            with open(os.path.join(self.work_dir, "_count_events_in.json")) as f:
                request = json.load(f)
            self.requests.append(request)
            if write:
                result = (
                    counts
                    if counts is not None
                    else {p: 1000 for p in request["paths"]}
                )
                with open(
                    os.path.join(self.work_dir, "_count_events_out.json"), "w"
                ) as f:
                    json.dump(result, f)
            return 0, "\n".join(list(stdout_lines)), ""

        return mock.patch("dsprod.tools.ps_call", side_effect=ps_call)

    def count(self, paths, **kwargs):
        with self.child(**kwargs):
            return run_step.count_events(PARAMS, paths, self.work_dir)


class TestOneInvocationPerGroup(CountCase):
    def test_a_whole_group_is_counted_in_one_call(self):
        paths = ["/store/nano_%d.root" % i for i in range(50)]
        counts = self.count(["/store/merged.root"] + paths)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(len(counts), 51)

    def test_one_path_on_its_own_is_refused(self):
        # the shape the single-file version took; iterating it would ask for the entry count of
        # every character of the path
        with self.assertRaises(TypeError):
            run_step.count_events(PARAMS, "/store/a.root", self.work_dir)

    def test_the_child_is_given_the_paths_in_a_file(self):
        # 50 remote paths on the command line, inside a `singularity --command-to-run '...'`
        # string, is a quoting problem nobody needs to have
        self.count(["/store/a.root", "/store/b.root"])
        self.assertEqual(self.requests[-1]["paths"], ["/store/a.root", "/store/b.root"])
        self.assertNotIn("/store/a.root", self.calls[0])


class TestTheResultIsKeyedByPath(CountCase):
    def test_counts_follow_the_requested_order_not_the_childs(self):
        # a dict is written in whatever order the child filled it; the caller compares its
        # merged file against the sum of its inputs, so a shuffled answer is a wrong verdict
        paths = ["/store/merged.root", "/store/a.root", "/store/b.root"]
        counts = self.count(
            paths,
            counts={
                "/store/b.root": 400,
                "/store/merged.root": 1000,
                "/store/a.root": 600,
            },
        )
        self.assertEqual(counts, [1000, 600, 400])

    def test_root_warnings_on_stdout_do_not_reach_the_counts(self):
        # the failure a positional read produces: `int(out.splitlines()[-1])` on this output
        # either raises or, worse, returns a number ROOT printed
        counts = self.count(
            ["/store/merged.root", "/store/a.root"],
            counts={"/store/merged.root": 2000, "/store/a.root": 2000},
            stdout_lines=ROOT_NOISE + ["1"] + ROOT_NOISE,
        )
        self.assertEqual(counts, [2000, 2000])


class TestAFailedCountIsReported(CountCase):
    def test_no_result_file_names_what_the_child_printed(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.count(["/store/a.root"], write=False, stdout_lines=ROOT_NOISE)
        msg = str(ctx.exception)
        self.assertIn("no readable result", msg)
        self.assertIn("trying to recover", msg)

    def test_a_missing_file_is_not_read_as_zero_events(self):
        # 0 would be reported as a merge that lost every event of the file, and the seeds it
        # blames would then be deleted and regenerated
        with self.assertRaises(RuntimeError) as ctx:
            self.count(
                ["/store/a.root", "/store/b.root"], counts={"/store/a.root": 1000}
            )
        self.assertIn("/store/b.root", str(ctx.exception))

    def test_a_stale_result_of_an_earlier_call_is_not_reused(self):
        with open(os.path.join(self.work_dir, "_count_events_out.json"), "w") as f:
            json.dump({"/store/a.root": 999}, f)
        with self.assertRaises(RuntimeError):
            self.count(["/store/a.root"], write=False)


#: enough of ROOT to run the child script for real, noise included: a file named `nano_<n>.root`
#: holds n entries
_FAKE_ROOT = """class _Tree:
    def __init__(self, n):
        self.n = n

    def GetEntries(self):
        return self.n


class _File:
    def __init__(self, path):
        self.path = path

    def IsZombie(self):
        return False

    def Get(self, name):
        print("Warning in <TFile::Init>: file probably not closed")
        if name != "Events":
            return None
        return _Tree(int(self.path.rsplit("_", 1)[1].split(".")[0]))

    def Close(self):
        pass


class TFile:
    @staticmethod
    def Open(path):
        print("Warning in <TClass::Init>: no dictionary for class edm::Hash<1>")
        return _File(path)
"""


class TestTheChildScript(unittest.TestCase):
    """The script itself, run for real against a stand-in ROOT.

    Nothing else executes it: it is a string in `run_step`, handed to a python inside a CMSSW
    release, so a mistake in it is found by a production job hours into a merge.
    """

    def setUp(self):
        self.work_dir = tempfile.mkdtemp(prefix="dsprod_child_test_")
        self.addCleanup(shutil.rmtree, self.work_dir, True)
        self.script = os.path.join(self.work_dir, "_count_events.py")
        with open(self.script, "w") as f:
            f.write(run_step._COUNT_EVENTS_SCRIPT)
        with open(os.path.join(self.work_dir, "ROOT.py"), "w") as f:
            f.write(_FAKE_ROOT)

    def run_child(self, paths, tree="Events"):
        request = os.path.join(self.work_dir, "in.json")
        self.result = os.path.join(self.work_dir, "out.json")
        with open(request, "w") as f:
            json.dump({"tree": tree, "paths": paths}, f)
        return subprocess.run(
            [sys.executable, self.script, request, self.result],
            capture_output=True,
            text=True,
            env=dict(os.environ, PYTHONPATH=self.work_dir),
        )

    def test_it_writes_the_count_of_every_requested_file(self):
        paths = ["/store/nano_1000.root", "/store/nano_250.root"]
        proc = self.run_child(paths)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        with open(self.result) as f:
            self.assertEqual(json.load(f), {paths[0]: 1000, paths[1]: 250})
        # and it really did print over its own answer
        self.assertIn("Warning in <TClass::Init>", proc.stdout)

    def test_it_refuses_a_file_without_the_tree(self):
        proc = self.run_child(["/store/nano_10.root"], tree="Runs")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("holds no Runs tree", proc.stderr)


if __name__ == "__main__":
    unittest.main()
