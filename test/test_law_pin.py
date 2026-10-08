#!/usr/bin/env python3
"""DSProd runs with exactly one law release, on the driver and on the grid workers alike.

law 0.1.21 (2026-10-07) renamed the CRAB job manager's `proxy` argument, and every submission and
status query of DSProd's 0.1.20-era code failed on it -- reported, by the retry path, as an
unreadable status. The release is therefore pinned, and the pin has to reach existing areas: a
guard that only asked "is a venv there?" kept every checkout on the law it was first built with,
while the driver's law renders the job scripts that the vendored copy runs on the worker.

Three things are pinned. The release is the same in `env.sh`, `dsprod/law_version.py` and CI.
Importing DSProd with another law stops with a message that says what to do -- and modules that
need no law still import without one. And `env.sh` rebuilds the driver venv and the vendored copy
when they hold another release, leaves them alone when they hold the pinned one, and refuses to
touch a directory it cannot read rather than guess. A law batch job neither builds nor rebuilds
either, and the vendored copy is moved into place only once pip has filled it. A step of either
build that fails stops `env.sh` short of `env.sh done!`, also in a shell that ignores SIGINT --
which `run_cmd` relies on to stop it. Those guards, and the `action` that passes their refusal on,
are exercised as written in `env.sh`, `run_cmd` included, with `pip`, the venv creation and the
cvmfs setup replaced.
"""

import itertools
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

import law  # noqa: E402

from dsprod.law_version import LAW_VERSION  # noqa: E402

ENV_SH = os.path.join(dsprod_repo, "env.sh")
UNIT_TESTS_YAML = os.path.join(dsprod_repo, ".github", "workflows", "unit-tests.yaml")

#: every module of the package that uses law
LAW_MODULES = (
    "dsprod.tasks",
    "dsprod.crab",
    "dsprod.tools",
    "dsprod.grid_tools",
    "dsprod.law_gfal",
    "dsprod.law_wlcg",
    "dsprod.watchdog",
)


def read(path):
    with open(path) as f:
        return f.read()


def python(code, **env):
    """Run `code` in a fresh interpreter from the checkout; returns the completed process."""
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=dsprod_repo,
        env=dict(
            os.environ,
            PYTHONPATH=dsprod_repo,
            PYTHONDONTWRITEBYTECODE="1",
            ANALYSIS_PATH=dsprod_repo,
            **env,
        ),
        capture_output=True,
        text=True,
    )


class ThePinIsOneRelease(unittest.TestCase):
    def test_the_imported_law_is_the_pinned_one(self):
        self.assertEqual(law.__version__, LAW_VERSION)

    def test_env_sh_installs_it_in_both_places_from_one_variable(self):
        env = read(ENV_SH)
        self.assertEqual(re.findall(r'local law_version="([^"]+)"', env), [LAW_VERSION])
        self.assertEqual(env.count('"law==$law_version"'), 2)
        # no second, literal pin that could drift away from the variable
        self.assertEqual(re.findall(r"law==\d", env), [])
        self.assertIn(
            '_dsprod_driver_env "$this_dir/soft/dsprod_env" "$law_version"', env
        )
        self.assertIn('_dsprod_vendor "$this_dir/soft/vendor" "$law_version"', env)

    def test_ci_installs_it(self):
        pins = re.findall(r"\blaw==([\w.]+)", read(UNIT_TESTS_YAML))
        self.assertEqual(pins, [LAW_VERSION])


class AnotherReleaseIsRefusedAtImport(unittest.TestCase):
    """`law.__version__` is what the check reads, so changing it before the import is the other
    release, as far as DSProd can tell."""

    IMPORT_EACH = """
        import importlib, json, law
        {patch}
        result = {{}}
        for name in {modules!r}:
            try:
                importlib.import_module(name)
                result[name] = None
            except Exception as exc:
                result[name] = f"{{type(exc).__name__}}: {{exc}}"
        print(json.dumps(result))
    """

    def import_each(self, patch=""):
        proc = python(self.IMPORT_EACH.format(patch=patch, modules=LAW_MODULES))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout.strip().splitlines()[-1])

    def test_every_module_that_uses_law_refuses_another_release(self):
        # an older release and a newer one: the pin is exact, not a minimum
        for other in ("0.1.20", "0.1.22"):
            result = self.import_each(patch=f"law.__version__ = {other!r}")
            for name in LAW_MODULES:
                with self.subTest(name, law=other):
                    self.assertIsNotNone(
                        result[name], f"{name} imported under law {other}"
                    )
                    self.assertIn("RuntimeError", result[name])
                    self.assertIn(
                        f"DSProd needs law {LAW_VERSION}, but law {other}", result[name]
                    )
                    self.assertIn("env.sh", result[name])

    def test_the_pinned_release_imports_them_all(self):
        self.assertEqual(self.import_each(), {name: None for name in LAW_MODULES})

    def test_what_needs_no_law_still_imports_without_it(self):
        """`run_tools/check_conditions.py` runs in a CI job that installs PyYAML only."""
        proc = python("""
            import sys
            sys.modules["law"] = None  # `import law` now raises ImportError
            import dsprod.config, dsprod.registry, dsprod.run_step
            try:
                import dsprod.tools
            except ImportError:
                print("law-free imports ok")
            """)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("law-free imports ok", proc.stdout)


#: stand-ins for what the guards must not really do: create a venv, run pip and set up cvmfs. The
#: other commands of the builds -- `rm`, `mv`, `find`, `touch` -- run for real, on scratch
#: directories; every one of them is logged, and FAKE_FAILS makes one fail:
#:   pip            pip is cut short: it leaves law's dist-info without the package, and fails
#:   pip_killed     pip dies (as under `kill -9`) before it has created anything
#:   venv           the venv is left without its activate script, and its creation fails
#:   rm             removing a directory fails, and nothing is removed
#:   stale_removal  removing what an unfinished vendoring left fails
#:   mv_aside       renaming the old vendored copy aside fails
#:   mv_into_place  renaming the new vendored copy into place fails
HARNESS = r"""
LOG="$1"; shift
/usr/bin/python3() {
  echo "/usr/bin/python3 $*" >> "$LOG"
  local d
  for d; do :; done
  mkdir -p "$d/bin" "$d/lib/python3.9/site-packages"
  printf '#!/bin/sh\necho "%s/lib/python3.9/site-packages"\n' "$d" > "$d/bin/python3"
  chmod +x "$d/bin/python3"
  [ "$FAKE_FAILS" = venv ] && return 1
  printf 'export VIRTUAL_ENV="%s"\n' "$d" > "$d/bin/activate"
}
pip() {
  echo "pip $*" >> "$LOG"
  local target="" version=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --target) target="$2"; shift ;;
      law==*) version="${1#law==}" ;;
    esac
    shift
  done
  [ -z "$version" ] && return 0
  [ "$FAKE_FAILS" = pip_killed ] && return 137
  [ -z "$target" ] && target="$VIRTUAL_ENV/lib/python3.9/site-packages"
  mkdir -p "$target/law-$version.dist-info"
  [ "$FAKE_FAILS" = pip ] && return 1
  mkdir -p "$target/law"
}
source() {
  if [ "$1" = /cvmfs/cms.cern.ch/cmsset_default.sh ]; then
    echo "source $1" >> "$LOG"
    return 0
  fi
  builtin source "$@"
}
rm() {
  echo "rm $*" >> "$LOG"
  [ "$FAKE_FAILS" = rm ] && return 1
  command rm "$@"
}
find() {
  echo "find $*" >> "$LOG"
  [ "$FAKE_FAILS" = stale_removal ] && [ "$2" = -maxdepth ] && return 1
  command find "$@"
}
mv() {
  echo "mv $*" >> "$LOG"
  [ "$FAKE_FAILS" = mv_aside ] && [ "${2%.old}" != "$2" ] && return 1
  [ "$FAKE_FAILS" = mv_into_place ] && [ "${2%.old}" = "$2" ] && return 1
  command mv "$@"
}
touch() {
  echo "touch $*" >> "$LOG"
  command touch "$@"
}
@FUNCTIONS@
"$@"
rc=$?
echo "rc=$rc venv=${VIRTUAL_ENV:-}" >> "$LOG"
exit $rc
"""


def env_sh_functions(*names):
    """The definitions of `names` as they stand in env.sh."""
    text = read(ENV_SH)
    found = []
    for name in names:
        match = re.search(
            rf"^(?:function {re.escape(name)} |{re.escape(name)}\(\) )\{{\n.*?^\}}\n",
            text,
            re.MULTILINE | re.DOTALL,
        )
        if match is None:
            raise AssertionError(f"env.sh defines no function {name}")
        found.append(match.group(0))
    return "\n".join(found)


class EnvShBringsAnAreaToThePin(unittest.TestCase):
    #: shells env.sh is sourced from; bash is what CI has
    shells = [s for s in ("bash", "zsh") if shutil.which(s)]
    #: how a shell takes SIGINT: interrupted by it (foreground, interactive), or ignoring it (any
    #: `cmd &` from a script, `setsid nohup ... &` included) -- in which case `run_cmd` does not
    #: stop it
    sigints = (signal.SIG_DFL, signal.SIG_IGN)

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="dsprod_env_guard_test_")
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.harness = os.path.join(self.dir, "harness.sh")
        with open(self.harness, "w") as f:
            f.write(
                HARNESS.replace(
                    "@FUNCTIONS@",
                    env_sh_functions(
                        "run_cmd",
                        "_dsprod_law_in",
                        "_dsprod_may_rebuild",
                        "_dsprod_may_build",
                        "_dsprod_driver_env",
                        "_dsprod_vendor",
                        "action",
                    ),
                )
            )
        self.soft = os.path.join(self.dir, "soft")
        os.makedirs(self.soft)
        self.venv = os.path.join(self.soft, "dsprod_env")
        self.vendor = os.path.join(self.soft, "vendor")

    def run_guard(
        self, shell, *call, in_law_job=False, sigint=signal.SIG_DFL, **extra_env
    ):
        """Runs `call` in the harness. `sigint` is how the shell takes SIGINT: interrupted by it,
        as an interactive or foreground shell is, or ignoring it, as a shell started with `&` from
        a script is -- `setsid nohup bash -c 'source env.sh' < /dev/null &` among them.
        """
        log = os.path.join(self.dir, f"log.{shell}")
        open(log, "w").close()
        env = dict(os.environ, VIRTUAL_ENV="")
        for name in ("LAW_JOB_HOME", "DSPROD_ON_GRID", "FAKE_FAILS"):
            env.pop(name, None)
        if in_law_job:
            env["LAW_JOB_HOME"] = os.path.join(self.dir, "job_home")
        env.update(extra_env)
        proc = subprocess.run(
            [shell, self.harness, log, *call],
            capture_output=True,
            text=True,
            env=env,
            preexec_fn=lambda: signal.signal(signal.SIGINT, sigint),
        )
        return proc, read(log).splitlines()

    def make_venv(self, law_version, installed=True):
        """A venv as the harness builds one, holding `law_version`."""
        site = os.path.join(self.venv, "lib", "python3.9", "site-packages")
        os.makedirs(site)
        os.makedirs(os.path.join(self.venv, "bin"))
        python3 = os.path.join(self.venv, "bin", "python3")
        with open(python3, "w") as f:
            f.write(f'#!/bin/sh\necho "{site}"\n')
        os.chmod(python3, 0o755)
        with open(os.path.join(self.venv, "bin", "activate"), "w") as f:
            f.write(f'export VIRTUAL_ENV="{self.venv}"\n')
        if law_version:
            os.makedirs(os.path.join(site, f"law-{law_version}.dist-info"))
        open(os.path.join(site, "left_from_before"), "w").close()
        if installed:
            open(os.path.join(self.venv, ".installed"), "w").close()
        return site

    def make_vendor(self, *law_versions):
        os.makedirs(os.path.join(self.vendor, "law"))
        for version in law_versions:
            os.makedirs(os.path.join(self.vendor, f"law-{version}.dist-info"))
        open(os.path.join(self.vendor, "left_from_before"), "w").close()

    def fresh_area(self):
        """Every shell's case starts from the same, empty area."""
        shutil.rmtree(self.soft)
        os.makedirs(self.soft)

    def tree(self):
        """Every path under soft/, to tell that nothing was created, removed or moved."""
        return sorted(
            os.path.relpath(os.path.join(root, name), self.soft)
            for root, dirs, files in os.walk(self.soft)
            for name in dirs + files
        )

    def vendored_releases(self):
        return sorted(p for p in os.listdir(self.vendor) if p.startswith("law"))

    def assertNothingRan(self, log):
        self.assertEqual([line for line in log if not line.startswith("rc=")], [])

    def vendor_tree(self):
        return [p for p in self.tree() if p.split(os.sep)[0] == "vendor"]

    def assertSourcedAgainVendorsACompleteCopy(self, shell, sigint):
        proc, _ = self.run_guard(shell, "action", sigint=sigint)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("env.sh done!", proc.stdout)
        self.assertEqual(
            self.vendored_releases(), ["law", f"law-{LAW_VERSION}.dist-info"]
        )
        self.assertFalse(os.path.exists(os.path.join(self.vendor, "left_from_before")))
        self.assertEqual(sorted(os.listdir(self.soft)), ["bin", "dsprod_env", "vendor"])

    # -- the vendored copy shipped to CRAB workers

    def test_a_vendored_copy_of_another_release_is_replaced(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_vendor("0.1.20")
                proc, log = self.run_guard(
                    shell, "_dsprod_vendor", self.vendor, LAW_VERSION
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(
                    self.vendored_releases(), ["law", f"law-{LAW_VERSION}.dist-info"]
                )
                self.assertFalse(
                    os.path.exists(os.path.join(self.vendor, "left_from_before"))
                )
                self.assertEqual(os.listdir(self.soft), ["vendor"])
                self.assertIn("holds law 0.1.20", proc.stdout)
                (pip,) = [line for line in log if line.startswith("pip ")]
                building = re.fullmatch(
                    rf"pip install --quiet --target ({re.escape(self.vendor)}\.tmp\.\d+) "
                    rf"luigi==3\.7\.3 law=={re.escape(LAW_VERSION)}",
                    pip,
                )
                self.assertIsNotNone(building, pip)
                # the old copy stays in place until pip has filled the sibling
                moved_aside = f"mv {self.vendor} {building[1]}.old"
                self.assertIn(moved_aside, log)
                self.assertLess(log.index(pip), log.index(moved_aside))

    def test_two_releases_side_by_side_are_another_release_too(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_vendor("0.1.20", LAW_VERSION)
                proc, _ = self.run_guard(
                    shell, "_dsprod_vendor", self.vendor, LAW_VERSION
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(
                    self.vendored_releases(), ["law", f"law-{LAW_VERSION}.dist-info"]
                )

    def test_a_vendored_copy_of_the_pinned_release_is_left_alone(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_vendor(LAW_VERSION)
                proc, log = self.run_guard(
                    shell, "_dsprod_vendor", self.vendor, LAW_VERSION
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertNothingRan(log)
                self.assertTrue(
                    os.path.exists(os.path.join(self.vendor, "left_from_before"))
                )

    def test_a_missing_vendored_copy_is_made(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                proc, _ = self.run_guard(
                    shell, "_dsprod_vendor", self.vendor, LAW_VERSION
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(
                    self.vendored_releases(), ["law", f"law-{LAW_VERSION}.dist-info"]
                )
                self.assertEqual(os.listdir(self.soft), ["vendor"])

    def test_a_vendoring_cut_short_leaves_nothing_half_filled_in_place(self):
        """A half-filled copy holding the pinned dist-info would pass the release check of every
        later source and be shipped to CRAB, so it must never stand where soft/vendor is read; nor
        may a pip that failed leave soft/vendor missing, which the code tarball would silently go
        without. Sourcing env.sh stops at the failure whether the shell takes SIGINT or not.
        """
        for shell, sigint, fails, old in itertools.product(
            self.shells, self.sigints, ("pip", "pip_killed"), (None, "0.1.20")
        ):
            with self.subTest(shell=shell, sigint=sigint.name, fails=fails, old=old):
                self.fresh_area()
                self.make_venv(LAW_VERSION)
                if old:
                    self.make_vendor(old)
                before = self.tree()
                proc, _ = self.run_guard(
                    shell, "action", sigint=sigint, FAKE_FAILS=fails
                )
                self.assertNotEqual(proc.returncode, 0)
                self.assertNotIn("env.sh done!", proc.stdout)
                self.assertIn(
                    f"ERROR: vendoring law + luigi failed; {self.vendor} is left as it was.",
                    proc.stderr,
                )
                # soft/vendor is as it was -- absent, or the old copy untouched -- and whatever
                # pip left is in the build's own sibling
                leftover = set(os.listdir(self.soft)) - {"dsprod_env", "vendor"}
                if fails == "pip":
                    (sibling,) = leftover
                    self.assertRegex(sibling, r"^vendor\.tmp\.\d+$")
                else:
                    self.assertEqual(leftover, set())
                self.assertEqual(
                    [p for p in self.tree() if p.split(os.sep)[0] not in leftover],
                    before,
                )
                # the next source removes it and vendors a complete copy
                self.assertSourcedAgainVendorsACompleteCopy(shell, sigint)

    def test_a_vendoring_step_that_fails_stops_env_sh(self):
        """Removing what an earlier vendoring left and the two renames are checked as pip is. In a
        shell that ignores SIGINT, `run_cmd` would carry on: into pip next to a leftover it could
        not remove, into renaming the new copy into the old one it could not move aside, or into
        deleting the old copy with no new one in its place -- and end in `env.sh done!`.
        """
        errors = {
            "stale_removal": "ERROR: cannot remove what an unfinished vendoring left next to "
            f"{self.vendor}; {self.vendor} is left as it was.",
            "mv_aside": f"ERROR: cannot move {self.vendor} aside to replace it; it is left as "
            "it was.",
            "mv_into_place": "ERROR: cannot move the new vendored copy into "
            f"{self.vendor}, which is missing until env.sh is sourced again.",
        }
        for shell, sigint, fails in itertools.product(
            self.shells, self.sigints, errors
        ):
            with self.subTest(shell=shell, sigint=sigint.name, fails=fails):
                self.fresh_area()
                self.make_venv(LAW_VERSION)
                self.make_vendor("0.1.20")
                os.makedirs(os.path.join(self.soft, "vendor.tmp.1", "law"))
                before = self.vendor_tree()
                proc, log = self.run_guard(
                    shell, "action", sigint=sigint, FAKE_FAILS=fails
                )
                self.assertNotEqual(proc.returncode, 0)
                self.assertNotIn("env.sh done!", proc.stdout)
                self.assertIn(errors[fails], proc.stderr)
                siblings = sorted(set(os.listdir(self.soft)) - {"dsprod_env", "vendor"})
                if fails == "stale_removal":
                    self.assertFalse([line for line in log if line.startswith("pip ")])
                    self.assertEqual(siblings, ["vendor.tmp.1"])
                if fails == "mv_into_place":
                    # the old copy is aside and the new one, complete, next to it
                    self.assertFalse(os.path.exists(self.vendor))
                    new, old = siblings
                    self.assertRegex(new, r"^vendor\.tmp\.\d+$")
                    self.assertEqual(old, f"{new}.old")
                    self.assertTrue(os.path.isdir(os.path.join(self.soft, new, "law")))
                    self.assertTrue(
                        os.path.exists(os.path.join(self.soft, old, "left_from_before"))
                    )
                else:
                    self.assertEqual(self.vendor_tree(), before)
                self.assertSourcedAgainVendorsACompleteCopy(shell, sigint)

    @unittest.skipIf(os.geteuid() == 0, "root lists any directory")
    def test_a_vendored_copy_that_cannot_be_read_is_not_guessed_at(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_vendor("0.1.20")
                os.chmod(self.vendor, 0)
                try:
                    proc, log = self.run_guard(
                        shell, "_dsprod_vendor", self.vendor, LAW_VERSION
                    )
                finally:
                    os.chmod(self.vendor, 0o755)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn("cannot tell which law", proc.stderr)
                self.assertNothingRan(log)
                self.assertTrue(
                    os.path.exists(os.path.join(self.vendor, "left_from_before"))
                )

    def test_a_batch_job_refuses_rather_than_rebuild_what_others_share(self):
        """HTCondor jobs source the checkout's env.sh: a pin pulled while they run must not have
        each one that starts delete the venv and the vendored copy under the others."""
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                site = self.make_venv("0.1.20")
                self.make_vendor("0.1.20")
                for call, kept in (
                    (("_dsprod_driver_env", self.venv), site),
                    (("_dsprod_vendor", self.vendor), self.vendor),
                ):
                    proc, log = self.run_guard(
                        shell, *call, LAW_VERSION, in_law_job=True
                    )
                    self.assertNotEqual(proc.returncode, 0)
                    self.assertIn("a batch job does not rebuild", proc.stderr)
                    self.assertIn(
                        "Source env.sh on the submitting machine", proc.stderr
                    )
                    self.assertNothingRan(log)
                    self.assertTrue(
                        os.path.exists(os.path.join(kept, "left_from_before"))
                    )

    def test_a_batch_job_with_the_pinned_release_runs_as_before(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_venv(LAW_VERSION)
                self.make_vendor(LAW_VERSION)
                for call in (
                    ("_dsprod_driver_env", self.venv),
                    ("_dsprod_vendor", self.vendor),
                ):
                    proc, _ = self.run_guard(shell, *call, LAW_VERSION, in_law_job=True)
                    self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_a_batch_job_never_builds_what_is_missing_or_unfinished(self):
        """A job that starts while env.sh rebuilds on the submitting machine finds the venv gone
        or without .installed, or soft/vendor gone; building them too would run venv and pip into
        the directories that rebuild is filling."""
        cases = {
            "no venv": ("_dsprod_driver_env", self.venv),
            "half-built venv": ("_dsprod_driver_env", self.venv),
            "no vendor": ("_dsprod_vendor", self.vendor),
        }
        for shell in self.shells:
            for case, call in cases.items():
                with self.subTest(shell=shell, case=case):
                    self.fresh_area()
                    if case == "half-built venv":
                        self.make_venv(None, installed=False)
                    before = self.tree()
                    proc, log = self.run_guard(
                        shell, *call, LAW_VERSION, in_law_job=True
                    )
                    self.assertNotEqual(proc.returncode, 0)
                    self.assertIn(
                        f"ERROR: {call[1]} is missing or unfinished", proc.stderr
                    )
                    self.assertIn("a batch job does not build", proc.stderr)
                    self.assertIn(
                        "Source env.sh on the submitting machine and resubmit",
                        proc.stderr,
                    )
                    self.assertNothingRan(log)
                    self.assertEqual(self.tree(), before)

    def test_a_batch_job_in_an_area_never_built_fails_env_sh(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                proc, log = self.run_guard(shell, "action", in_law_job=True)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn(f"ERROR: {self.venv} is missing", proc.stderr)
                self.assertNotIn("env.sh done!", proc.stdout)
                self.assertNothingRan(log)
                self.assertEqual(self.tree(), [])

    def test_a_crab_worker_neither_builds_nor_needs_a_build(self):
        """The grid branch runs the vendored copy shipped in the code tarball. Sent through the
        guards, every CRAB job -- a law batch job in an area with no soft/dsprod_env -- would stop
        at env.sh."""
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                proc, log = self.run_guard(
                    shell, "action", in_law_job=True, DSPROD_ON_GRID="1"
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                self.assertNotIn("ERROR", proc.stderr)
                # the CRAB-provided cvmfs setup is all that runs
                self.assertEqual(
                    log[:-1], ["source /cvmfs/cms.cern.ch/cmsset_default.sh"]
                )
                self.assertNotIn("dsprod_env", os.listdir(self.soft))
                self.assertFalse(
                    [p for p in os.listdir(self.soft) if p.startswith("vendor")]
                )

    # -- the driver venv

    def test_a_venv_with_another_release_is_rebuilt(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                site = self.make_venv("0.1.20")
                proc, log = self.run_guard(
                    shell, "_dsprod_driver_env", self.venv, LAW_VERSION
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn(f"rm -rf {self.venv}", log)
                self.assertIn(
                    f"/usr/bin/python3 -m venv --system-site-packages {self.venv}", log
                )
                pip = f"pip install --quiet --ignore-installed luigi==3.7.3 law=={LAW_VERSION}"
                self.assertIn(pip, log)
                # marked finished only once law is in it
                self.assertLess(
                    log.index(pip), log.index(f"touch {self.venv}/.installed")
                )
                self.assertEqual(
                    sorted(p for p in os.listdir(site) if p.startswith("law")),
                    ["law", f"law-{LAW_VERSION}.dist-info"],
                )
                self.assertFalse(os.path.exists(os.path.join(site, "left_from_before")))
                self.assertTrue(os.path.exists(os.path.join(self.venv, ".installed")))
                self.assertEqual(log[-1], f"rc=0 venv={self.venv}")

    def test_a_venv_with_the_pinned_release_is_only_activated(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                site = self.make_venv(LAW_VERSION)
                proc, log = self.run_guard(
                    shell, "_dsprod_driver_env", self.venv, LAW_VERSION
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(log, [f"rc=0 venv={self.venv}"])
                self.assertTrue(os.path.exists(os.path.join(site, "left_from_before")))

    def test_a_venv_never_finished_is_built(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_venv(None, installed=False)
                proc, log = self.run_guard(
                    shell, "_dsprod_driver_env", self.venv, LAW_VERSION
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertFalse(any(line.startswith("rm ") for line in log))
                self.assertTrue(os.path.exists(os.path.join(self.venv, ".installed")))

    def test_a_venv_whose_python_does_not_answer_is_not_guessed_at(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_venv("0.1.20")
                with open(os.path.join(self.venv, "bin", "python3"), "w") as f:
                    f.write("#!/bin/sh\nexit 1\n")
                proc, log = self.run_guard(
                    shell, "_dsprod_driver_env", self.venv, LAW_VERSION
                )
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn("cannot tell which law", proc.stderr)
                self.assertNothingRan(log)
                self.assertTrue(os.path.exists(os.path.join(self.venv, ".installed")))

    @unittest.skipIf(os.geteuid() == 0, "root lists any directory")
    def test_a_venv_whose_site_packages_cannot_be_read_is_not_guessed_at(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                site = self.make_venv("0.1.20")
                os.chmod(site, 0)
                try:
                    proc, log = self.run_guard(
                        shell, "_dsprod_driver_env", self.venv, LAW_VERSION
                    )
                finally:
                    os.chmod(site, 0o755)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn("cannot tell which law", proc.stderr)
                self.assertNothingRan(log)
                self.assertTrue(os.path.exists(os.path.join(self.venv, ".installed")))
                self.assertTrue(os.path.exists(os.path.join(site, "left_from_before")))

    def test_a_venv_build_step_that_fails_stops_env_sh(self):
        """The venv is marked finished only once law is in it, and sourcing env.sh stops at the
        step that failed whether the shell takes SIGINT or not. In a shell that ignores it,
        `run_cmd` would carry on: into activating the old venv it could not remove, into pip
        outside a venv that was never made, or into writing .installed next to a pip that failed
        -- and end in `env.sh done!`."""
        cases = {
            # failing step: (law in the venv before, error)
            "rm": ("0.1.20", f"ERROR: cannot remove {self.venv} to rebuild it."),
            "venv": (None, f"ERROR: creating the venv {self.venv} failed;"),
            "pip": (
                "0.1.20",
                f"ERROR: installing law + luigi into {self.venv} failed;",
            ),
        }
        for shell, sigint, fails in itertools.product(self.shells, self.sigints, cases):
            with self.subTest(shell=shell, sigint=sigint.name, fails=fails):
                law_before, error = cases[fails]
                self.fresh_area()
                if law_before:
                    self.make_venv(law_before)
                self.make_vendor(LAW_VERSION)
                before = self.tree()
                proc, log = self.run_guard(
                    shell, "action", sigint=sigint, FAKE_FAILS=fails
                )
                self.assertNotEqual(proc.returncode, 0)
                self.assertNotIn("env.sh done!", proc.stdout)
                self.assertIn(error, proc.stderr)
                self.assertFalse([line for line in log if line.startswith("touch ")])
                ran = [line.split()[0] for line in log if not line.startswith("rc=")]
                if fails == "rm":
                    self.assertEqual(ran, ["rm"])
                    self.assertEqual(self.tree(), before)
                else:
                    self.assertFalse(
                        os.path.exists(os.path.join(self.venv, ".installed"))
                    )
                    self.assertEqual(
                        ran[-1], {"venv": "/usr/bin/python3"}.get(fails, "pip")
                    )
                # the next source builds it
                proc, _ = self.run_guard(shell, "action", sigint=sigint)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                self.assertTrue(os.path.exists(os.path.join(self.venv, ".installed")))
                site = os.path.join(self.venv, "lib", "python3.9", "site-packages")
                self.assertEqual(
                    sorted(p for p in os.listdir(site) if p.startswith("law")),
                    ["law", f"law-{LAW_VERSION}.dist-info"],
                )

    # -- action, which runs both guards when env.sh is sourced

    def test_a_refusal_fails_env_sh_rather_than_being_carried_past(self):
        """Without `|| return 1`, sourcing env.sh would go on to the shims and "env.sh done!"
        with an environment that was refused, and the failure would surface later and elsewhere.
        """
        cases = (
            ("0.1.20", LAW_VERSION, self.venv),
            (LAW_VERSION, "0.1.20", self.vendor),
        )
        for shell in self.shells:
            for venv_law, vendor_law, refusing in cases:
                with self.subTest(shell=shell, refusing=os.path.basename(refusing)):
                    self.fresh_area()
                    self.make_venv(venv_law)
                    self.make_vendor(vendor_law)
                    proc, _ = self.run_guard(shell, "action", in_law_job=True)
                    self.assertNotEqual(proc.returncode, 0)
                    self.assertIn(f"ERROR: {refusing} holds law 0.1.20", proc.stderr)
                    self.assertNotIn("env.sh done!", proc.stdout)


class TheCrabSandboxStaysWhereItWas(unittest.TestCase):
    """law 0.1.21 moved its default CRAB sandbox from CMSSW_14_0_0 to CMSSW_14_2_1; law.cfg keeps
    the release law runs `crab` in, so no second release is installed under .law/cms/cmssw.
    """

    def test_law_reads_the_release_from_law_cfg(self):
        proc = python(
            """
            import law
            law.contrib.load("cms")
            print(law.config.Config.instance().get_expanded("job", "crab_sandbox_name"))
            """,
            LAW_CONFIG_FILE=os.path.join(dsprod_repo, "config", "law.cfg"),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(
            proc.stdout.strip().splitlines()[-1], "CMSSW_14_0_0::arch=el9_amd64_gcc12"
        )


if __name__ == "__main__":
    unittest.main()
