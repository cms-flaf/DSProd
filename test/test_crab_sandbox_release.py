#!/usr/bin/env python3
"""law 0.1.21 moved its default CRAB sandbox from CMSSW_14_0_0 to CMSSW_14_2_1; law.cfg keeps the
release law runs `crab` in, so no second release is installed under .law/cms/cmssw.
"""

import os
import subprocess
import sys
import textwrap
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TheCrabSandboxStaysWhereItWas(unittest.TestCase):
    def test_law_reads_the_release_from_law_cfg(self):
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent("""
                    import law
                    law.contrib.load("cms")
                    print(law.config.Config.instance().get_expanded("job", "crab_sandbox_name"))
                    """),
            ],
            cwd=dsprod_repo,
            env=dict(
                os.environ,
                PYTHONPATH=dsprod_repo,
                PYTHONDONTWRITEBYTECODE="1",
                ANALYSIS_PATH=dsprod_repo,
                LAW_CONFIG_FILE=os.path.join(dsprod_repo, "config", "law.cfg"),
            ),
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(
            proc.stdout.strip().splitlines()[-1], "CMSSW_14_0_0::arch=el9_amd64_gcc12"
        )


if __name__ == "__main__":
    unittest.main()
