"""What `run_tools/drive.sh` must never do to a production it does not own.

Two drivers polling one production area submit the same branches twice, and a driver that renews a
credential can leave the production with none at all (CreateVomsProxy removes the proxy before
creating a new one, and `voms-proxy-init` cannot run unattended). Both are guarded in the script,
so both are checked here by running the real script against a throwaway area with a fake `law`.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DRIVE = os.path.join(REPO, "run_tools", "drive.sh")

FAKE_LAW = """#!/bin/bash
n=$(cat "$FAKE_LAW_COUNTER" 2> /dev/null || echo 0)
echo $((n + 1)) > "$FAKE_LAW_COUNTER"
codes=($FAKE_LAW_CODES)
exit ${codes[$n]:-0}
"""

FAKE_VOMS_PROXY_INFO = """#!/bin/bash
for a in "$@"; do
  case $a in
    --timeleft) echo "${FAKE_PROXY_LEFT:-604800}"; exit 0 ;;
    --identity) echo "/DC=ch/DC=cern/CN=nobody"; exit 0 ;;
  esac
done
exit 0
"""


def host():
    """The host name the way drive.sh writes it into the lock."""
    for cmd in (["hostname", "-f"], ["hostname"]):
        try:
            out = subprocess.run(cmd, capture_output=True, text=True)
        except OSError:
            continue
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    raise unittest.SkipTest("no host name available")


def dead_pid():
    for pid in (4194303, 999999, 899999):
        if not os.path.exists(f"/proc/{pid}"):
            return pid
    raise unittest.SkipTest("no unused pid found")


@unittest.skipUnless(sys.platform.startswith("linux"), "drive.sh is run on Linux nodes")
class DriveTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.area = os.path.join(self.tmp.name, "area")
        os.makedirs(os.path.join(self.area, "run_tools"))
        # a copy, so that the script resolves its data/ inside the throwaway area
        shutil.copy(DRIVE, os.path.join(self.area, "run_tools", "drive.sh"))
        self.lock = os.path.join(self.area, "data", "driver.lock")
        self.counter = os.path.join(self.tmp.name, "law_calls")
        self.proxy = os.path.join(self.tmp.name, "voms.proxy")
        with open(self.proxy, "w") as f:
            f.write("a proxy nobody may touch")
        bin_dir = os.path.join(self.tmp.name, "bin")
        os.makedirs(bin_dir)
        for name, body in (
            ("law", FAKE_LAW),
            ("voms-proxy-info", FAKE_VOMS_PROXY_INFO),
        ):
            path = os.path.join(bin_dir, name)
            with open(path, "w") as f:
                f.write(body)
            os.chmod(path, 0o755)
        self.env = dict(os.environ)
        self.env.pop("ANALYSIS_PATH", None)
        self.env.update(
            PATH=bin_dir + os.pathsep + self.env.get("PATH", ""),
            X509_USER_PROXY=self.proxy,
            FAKE_LAW_COUNTER=self.counter,
            FAKE_LAW_CODES="0",
            DRIVE_BACKOFF_SECONDS="0",
            DRIVE_BACKOFF_MAX_SECONDS="0",
        )

    def tearDown(self):
        self.tmp.cleanup()

    def drive(self, *args, **env):
        self.env.update(env)
        run = subprocess.run(
            [os.path.join(self.area, "run_tools", "drive.sh")]
            + list(args or ["law", "run", "X"]),
            capture_output=True,
            text=True,
            env=self.env,
            timeout=120,
        )
        return run.returncode, run.stdout + run.stderr

    def law_calls(self):
        try:
            with open(self.counter) as f:
                return int(f.read())
        except OSError:
            return 0

    def write_lock(self, owner_host, pid, age_seconds):
        os.makedirs(self.lock)
        with open(os.path.join(self.lock, "owner"), "w") as f:
            f.write(
                f"host {owner_host}\npid {pid}\n"
                f"started_epoch {int(time.time()) - age_seconds}\n"
                "started earlier\ncommand law run X\n"
            )


class TestTheLock(DriveTestCase):
    def test_a_finished_run_leaves_no_lock_behind(self):
        code, text = self.drive()
        self.assertEqual(code, 0, text)
        self.assertFalse(os.path.exists(self.lock), text)
        self.assertEqual(self.law_calls(), 1)

    def test_a_live_lock_is_never_taken(self):
        # the pid of this test process: alive, on this host
        self.write_lock(host(), os.getpid(), 10 * 3600)
        code, text = self.drive()
        self.assertEqual(code, 4, text)
        self.assertIn("REFUSING", text)
        self.assertEqual(
            self.law_calls(), 0, "law must not run while another driver holds the lock"
        )
        self.assertTrue(os.path.exists(self.lock))

    def test_a_lock_from_another_host_is_left_alone(self):
        # the production area is shared, so a pid missing here says nothing about that host --
        # even a long-dead-looking lock must not be stolen
        self.write_lock("another-host.invalid", dead_pid(), 10 * 3600)
        code, text = self.drive()
        self.assertEqual(code, 4, text)
        self.assertIn("another-host.invalid", text)
        self.assertEqual(self.law_calls(), 0)
        self.assertTrue(os.path.exists(self.lock))

    def test_a_fresh_lock_whose_process_is_gone_is_still_left_alone(self):
        # a lock younger than the threshold is more likely half-written than abandoned
        self.write_lock(host(), dead_pid(), 60)
        code, text = self.drive()
        self.assertEqual(code, 4, text)
        self.assertEqual(self.law_calls(), 0)

    def test_a_stale_lock_is_taken_over(self):
        self.write_lock(host(), dead_pid(), 10 * 3600)
        code, text = self.drive()
        self.assertEqual(code, 0, text)
        self.assertIn("stale", text)
        self.assertEqual(self.law_calls(), 1)


class TestRestarts(DriveTestCase):
    def test_a_failed_task_is_resumed(self):
        # the normal way a leg of a long production ends; a driver that stops here is useless
        code, text = self.drive("law", "run", "X", FAKE_LAW_CODES="40 0")
        self.assertEqual(code, 0, text)
        self.assertEqual(self.law_calls(), 2)

    def test_a_law_abort_is_not_resumed(self):
        # exit 1 is law refusing the command itself (unknown task family): retrying is pointless
        code, text = self.drive("law", "run", "X", FAKE_LAW_CODES="1 0")
        self.assertEqual(code, 1, text)
        self.assertEqual(self.law_calls(), 1)
        self.assertFalse(os.path.exists(self.lock))


class TestCredentials(DriveTestCase):
    def test_a_short_proxy_stops_the_run_and_is_left_untouched(self):
        code, text = self.drive("law", "run", "X", FAKE_PROXY_LEFT="3600")
        self.assertEqual(code, 3, text)
        self.assertIn("REFUSING", text)
        self.assertEqual(self.law_calls(), 0)
        with open(self.proxy) as f:
            self.assertEqual(f.read(), "a proxy nobody may touch")

    def test_the_refusal_says_how_to_fix_it(self):
        _, text = self.drive("law", "run", "X", FAKE_PROXY_LEFT="3600")
        self.assertIn("voms-proxy-init", text)


if __name__ == "__main__":
    unittest.main()
