#!/usr/bin/env python3
"""The CRAB config DSProd submits transfers nothing, and carries nothing modern CRAB rejects.

DSProd writes every product and log itself, so CRAB's stageout must be off in the config that is
actually submitted -- the one law has already written to disk when `DSProdCrabJobFileFactory`
gets it back, which is why the file is rewritten rather than the config object edited. Nor does
the file carry `JobType.sendPythonFolder`, which modern CRAB refuses: law 0.1.21 does not write
it, so the rewrite leaves it alone. Both are pinned on a job file built by law's real factory.
"""

import os
import shutil
import sys
import tempfile
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

from law.job.base import JobInputFile  # noqa: E402

from dsprod.crab import DSProdCrabJobFileFactory  # noqa: E402


class TheSubmittedConfig(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="dsprod_crab_job_file_test_")
        self.addCleanup(shutil.rmtree, self.dir, True)

    def build(self):
        executable = os.path.join(self.dir, "law_job.sh")
        with open(executable, "w") as f:
            f.write("#!/bin/bash\necho {{render_variables}}\n")
        factory = DSProdCrabJobFileFactory(dir=self.dir, mkdtemp=False, cleanup=False)
        job_file, _ = factory(
            executable=JobInputFile(executable, copy=False, render_job=True),
            input_files={
                "job_file": JobInputFile(executable, copy=False, render_job=True)
            },
            arguments=["a", "b"],
            request_name="dsprod_test",
            work_area=self.dir,
            storage_site="T3_CH_CERNBOX",
            output_lfn_base="/store/user/someone/DSProd_crab_unused",
            # what a production task would ask CRAB to bring back, were it allowed to
            output_files=["nano.root"],
            render_variables={},
        )
        with open(job_file) as f:
            return f.read().splitlines()

    def test_no_crab_side_transfer_is_configured(self):
        lines = self.build()
        self.assertIn("cfg.General.transferOutputs = False", lines)
        self.assertIn("cfg.General.transferLogs = False", lines)
        self.assertFalse([ln for ln in lines if "JobType.outputFiles" in ln])

    def test_law_writes_no_send_python_folder(self):
        """Why the rewrite does not strip it: there is nothing to strip."""
        self.assertFalse([ln for ln in self.build() if "sendPythonFolder" in ln])

    def test_the_rewrite_turns_off_what_law_would_have_turned_on(self):
        """Independent of what this law release writes: the rewrite itself is the guarantee."""
        job_file = os.path.join(self.dir, "crab_job.py")
        with open(job_file, "w") as f:
            f.write(
                "cfg.General.requestName = 'x'\n"
                "cfg.General.transferOutputs = True\n"
                "cfg.General.transferLogs = True\n"
                "cfg.JobType.outputFiles = [\n"
                "    'nano.root',\n"
                "]\n"
                "cfg.JobType.scriptExe = 'crab_wrapper.sh'\n"
            )
        DSProdCrabJobFileFactory._rewrite_crab_job_file(job_file)
        with open(job_file) as f:
            self.assertEqual(
                f.read().splitlines(),
                [
                    "cfg.General.requestName = 'x'",
                    "cfg.General.transferOutputs = False",
                    "cfg.General.transferLogs = False",
                    "cfg.JobType.scriptExe = 'crab_wrapper.sh'",
                ],
            )


if __name__ == "__main__":
    unittest.main()
