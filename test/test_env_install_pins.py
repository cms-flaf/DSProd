#!/usr/bin/env python3
"""What `env.sh` installs is one explicit list of pins, and an area is rebuilt when that list changes.

`env.sh` installs pip, luigi and law into the driver venv `soft/dsprod_env` and, with the venv's
pip, into `soft/vendor`, the copy shipped to CRAB workers. Each is pinned to one release in a
single list, `dsprod_pins`, which both builds take and CI installs too. Each of the two
directories records that list in its marker `.installed`, written last; the next source of
`env.sh` leaves a directory whose marker matches the list alone and builds it again from scratch
when the marker is missing or records another list. So a pin change reaches existing areas, and
nothing in `env.sh` reads which release of which package a directory holds.

A law batch job never builds or rebuilds either directory. Whether a directory exists is read
once, from a listing of `soft/`, and acted on alone; a listing, lookup or marker that fails or
disagrees with itself stops `env.sh` with the directory left alone, and so does an entry that a
build finds where it creates the venv or moves the vendored copy. The vendored copy is moved into
place only once pip has filled it and it is marked, and a build step that fails stops `env.sh`
short of `env.sh done!`, also in a shell that ignores SIGINT -- which `run_cmd` relies on to stop
it. Those functions, and
the `action` that passes their refusal on, are exercised as written in `env.sh`, `run_cmd`
included, with `pip`, the venv creation and the cvmfs setup replaced.
"""

import itertools
import os
import re
import shutil
import signal
import subprocess
import tempfile
import unittest

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_SH = os.path.join(dsprod_repo, "env.sh")
UNIT_TESTS_YAML = os.path.join(dsprod_repo, ".github", "workflows", "unit-tests.yaml")

#: the functions that build the two directories
INSTALL_FUNCTIONS = (
    "_dsprod_installed",
    "_dsprod_may_build",
    "_dsprod_driver_env",
    "_dsprod_vendor",
)

PIN = re.compile(r"[A-Za-z][\w.-]*==[0-9][\w.]*")


def read(path):
    with open(path) as f:
        return f.read()


def env_sh_pins():
    (line,) = re.findall(r"^ *local -a dsprod_pins=\(([^)]*)\)$", read(ENV_SH), re.M)
    return line.split()


PINS = env_sh_pins()
#: what a directory built from PINS records in its marker
MARKER = " ".join(PINS) + "\n"
#: the same list with one release changed, as after a pull that moves a pin
OTHER = " ".join(PINS[:-1] + [PINS[-1].split("==")[0] + "==0.0.1"]) + "\n"


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


class ThePinsAreOneExplicitList(unittest.TestCase):
    def test_every_package_is_pinned_once_in_one_list(self):
        self.assertGreaterEqual(len(PINS), 2)
        for pin in PINS:
            self.assertRegex(pin, rf"^{PIN.pattern}$")
        names = [pin.split("==")[0] for pin in PINS]
        self.assertEqual(len(set(names)), len(names), PINS)
        self.assertEqual(names[0], "pip", "the venv's installer comes first")
        # no second, literal pin that could drift away from the list
        self.assertEqual(sorted(PIN.findall(read(ENV_SH))), sorted(PINS))

    def test_both_builds_take_the_list(self):
        env = read(ENV_SH)
        self.assertIn(
            '_dsprod_driver_env "$this_dir/soft/dsprod_env" "${dsprod_pins[@]}" || return 1',
            env,
        )
        self.assertIn(
            '_dsprod_vendor "$this_dir/soft/vendor" "${dsprod_pins[@]}" || return 1',
            env,
        )

    def test_nothing_else_is_installed(self):
        """Every pip install in env.sh installs entries of the list, and nothing else."""
        installs = re.findall(r"^ *(?:run_cmd )?pip install .*$", read(ENV_SH), re.M)
        self.assertEqual(
            [line.strip() for line in installs],
            [
                'pip install --quiet --upgrade "$1" || {',
                'pip install --quiet --ignore-installed "${@:2}" || {',
                'pip install --quiet --target "$building" "${@:2}" || {',
            ],
        )

    def test_the_install_functions_name_no_package(self):
        """No per-package probe or path: the packages are named in the list only."""
        names = [pin.split("==")[0] for pin in PINS[1:]]
        code = [
            line
            for line in env_sh_functions(*INSTALL_FUNCTIONS).splitlines()
            if not line.lstrip().startswith("#")
        ]
        for name in names:
            with self.subTest(name=name):
                self.assertEqual(
                    [
                        line
                        for line in code
                        if re.search(rf"\b{re.escape(name)}\b", line)
                    ],
                    [],
                )

    def test_ci_installs_the_same_pins(self):
        """CI pins exactly what env.sh pins, and a package env.sh pins is never installed
        unpinned there."""
        names = {pin.split("==")[0].lower() for pin in PINS}
        ci_pins = []
        for line in re.findall(r"\bpip install ([^\n]*)", read(UNIT_TESTS_YAML)):
            for word in line.split():
                if word.startswith("-"):
                    continue
                if PIN.fullmatch(word):
                    ci_pins.append(word)
                else:
                    self.assertNotIn(word.lower(), names, f"{word} is unpinned in CI")
        self.assertEqual(sorted(ci_pins), sorted(PINS))


#: stand-ins for what the tests must not really do: create a venv, run pip and set up cvmfs. The
#: other commands of the builds -- `rm`, `mv`, `find`, `ls`, `cat` -- run for real, on scratch
#: directories; `rm`, `mv` and `find` are logged, and FAKE_FAILS makes a step fail:
#:   venv           the venv is left without its activate script, and its creation fails
#:   pip_upgrade    upgrading the venv's pip fails
#:   pip            pip is cut short: it leaves the first package's dist-info only, and fails
#:   pip_killed     pip dies (as under `kill -9`) before it has created anything
#:   marker         the marker cannot be written: a directory stands where it goes
#:   rm             removing a directory fails, and nothing is removed
#:   stale_removal  removing what an unfinished vendoring left fails
#:   mv_aside       renaming the old vendored copy aside fails
#:   mv_into_place  renaming the new vendored copy into place fails
#: and FAKE_ALIASES defines, before env.sh's functions are read, aliases that an interactive shell
#: sourcing env.sh may carry for the commands that read a marker. CALLER_IFS is the IFS such a
#: shell may have set, in effect from then on; the IFS left after the call is then printed.
#: FAKE_STAT_FAILS=<path>:<n> answers the n-th `[` or `test` that names <path> as if <path> could
#: not be looked up -- a stat that fails once, as EOS can -- and counts in $LOG.stat how many name
#: it.
HARNESS = r"""
LOG="$1"; shift
if [ -n "$FAKE_ALIASES" ]; then
  [ -n "$BASH_VERSION" ] && shopt -s expand_aliases
  alias ls=false grep=false cat=false
fi
if [ -n "$CALLER_IFS" ]; then
  IFS="$CALLER_IFS"
fi
if [ -n "$FAKE_STAT_FAILS" ]; then
  printf 0 > "$LOG.stat"
  _fake_stat() {
    local cmd="$1" arg n
    local -a args=()
    shift
    for arg; do
      if builtin test "X$arg" = "X${FAKE_STAT_FAILS%:*}"; then
        n=$(( $(command cat "$LOG.stat") + 1 ))
        printf '%s' "$n" > "$LOG.stat"
        builtin test "$n" = "${FAKE_STAT_FAILS##*:}" && arg="$arg.unanswered"
      fi
      args+=("$arg")
    done
    builtin "$cmd" "${args[@]}"
  }
  function [ { _fake_stat [ "$@"; }
  function test { _fake_stat test "$@"; }
fi
/usr/bin/python3() {
  echo "/usr/bin/python3 $*" >> "$LOG"
  local d
  for d; do :; done
  mkdir -p "$d/bin" "$d/lib/python3.9/site-packages"
  [ "$FAKE_FAILS" = venv ] && return 1
  [ "$FAKE_FAILS" = marker ] && mkdir -p "$d/.installed"
  [ "$FAKE_FAILS" = activate ] && return 0
  printf 'export VIRTUAL_ENV="%s"\n' "$d" > "$d/bin/activate"
}
pip() {
  echo "pip $*" >> "$LOG"
  local target="" upgrade="" spec
  local -a specs=()
  while [ $# -gt 0 ]; do
    case "$1" in
      --target) target="$2"; shift ;;
      --upgrade) upgrade=1 ;;
      install|-*) ;;
      *) specs+=("$1") ;;
    esac
    shift
  done
  if [ -n "$upgrade" ]; then
    [ "$FAKE_FAILS" = pip_upgrade ] && return 1
    return 0
  fi
  [ "$FAKE_FAILS" = pip_killed ] && return 137
  if [ -n "$target" ]; then
    [ "$FAKE_FAILS" = marker ] && mkdir -p "$target/.installed"
  else
    target="$VIRTUAL_ENV/lib/python3.9/site-packages"
  fi
  for spec in "${specs[@]}"; do
    mkdir -p "$target/${spec%%==*}-${spec#*==}.dist-info"
    [ "$FAKE_FAILS" = pip ] && return 1
    mkdir -p "$target/${spec%%==*}"
  done
  return 0
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
  local arg
  for arg; do
    [ "$FAKE_FAILS" = stale_removal ] && [ "$arg" = -maxdepth ] && return 1
  done
  command find "$@"
}
mv() {
  echo "mv $*" >> "$LOG"
  local dest
  for dest; do :; done
  [ "$FAKE_FAILS" = mv_aside ] && [ "${dest%.old}" != "$dest" ] && return 1
  [ "$FAKE_FAILS" = mv_into_place ] && [ "${dest%.old}" = "$dest" ] && return 1
  command mv "$@"
}
@FUNCTIONS@
"$@"
rc=$?
if [ -n "$CALLER_IFS" ]; then
  printf 'IFS after the call: [%s]\n' "$IFS"
fi
echo "rc=$rc venv=${VIRTUAL_ENV:-}" >> "$LOG"
exit $rc
"""

#: a stand-in for `ls` on PATH, which `command ls` reaches as well:
#:   FAKE_LS="fail <dir>"         listing <dir> fails, as storage that does not answer makes it
#:   FAKE_LS="omit <name>"        a listing leaves <name> out
#:   FAKE_LS="omit <dir>/<name>"  the listing of <dir> leaves <name> out
FAKE_LS = r"""#!/usr/bin/env bash
for last; do :; done
case "$FAKE_LS" in
  "fail $last")
    echo "ls: cannot open directory '$last': Input/output error" >&2
    exit 2
    ;;
  "omit $last/"*)
    out="$(@LS@ "$@")" || exit
    printf '%s\n' "$out" | @GREP@ -vxF -- "${FAKE_LS#"omit $last/"}"
    exit 0
    ;;
  "omit "*)
    out="$(@LS@ "$@")" || exit
    printf '%s\n' "$out" | @GREP@ -vxF -- "${FAKE_LS#omit }"
    exit 0
    ;;
esac
exec @LS@ "$@"
"""


class EnvShBuildsWhatThePinsName(unittest.TestCase):
    #: shells env.sh is sourced from; under CI a missing zsh fails its cases rather than skipping them
    shells = (
        ["bash", "zsh"] if shutil.which("zsh") or os.environ.get("CI") else ["bash"]
    )
    #: how a shell takes SIGINT: interrupted by it (foreground, interactive), or ignoring it (any
    #: `cmd &` from a script, `setsid nohup ... &` included) -- in which case `run_cmd` does not
    #: stop it
    sigints = (signal.SIG_DFL, signal.SIG_IGN)

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="dsprod_env_pins_test_")
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.harness = os.path.join(self.dir, "harness.sh")
        with open(self.harness, "w") as f:
            f.write(
                HARNESS.replace(
                    "@FUNCTIONS@",
                    env_sh_functions("run_cmd", *INSTALL_FUNCTIONS, "action"),
                )
            )
        self.bin = os.path.join(self.dir, "fake_bin")
        os.makedirs(self.bin)
        path = os.path.join(self.bin, "ls")
        with open(path, "w") as f:
            f.write(
                FAKE_LS.replace("@LS@", shutil.which("ls")).replace(
                    "@GREP@", shutil.which("grep")
                )
            )
        os.chmod(path, 0o755)
        self.soft = os.path.join(self.dir, "soft")
        os.makedirs(self.soft)
        self.venv = os.path.join(self.soft, "dsprod_env")
        self.site = os.path.join(self.venv, "lib", "python3.9", "site-packages")
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
        env = dict(
            os.environ,
            VIRTUAL_ENV="",
            PATH=self.bin + os.pathsep + os.environ.get("PATH", ""),
        )
        for name in (
            "LAW_JOB_HOME",
            "DSPROD_ON_GRID",
            "FAKE_FAILS",
            "FAKE_ALIASES",
            "CALLER_IFS",
            "FAKE_STAT_FAILS",
            "FAKE_LS",
            "QUOTING_STYLE",
        ):
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

    @staticmethod
    def write_marker(path, marker):
        if marker is not None:
            with open(os.path.join(path, ".installed"), "w") as f:
                f.write(marker)

    def make_venv(self, marker):
        """A venv as the harness builds one, with the marker `marker` (None: no marker)."""
        os.makedirs(self.site)
        os.makedirs(os.path.join(self.venv, "bin"))
        with open(os.path.join(self.venv, "bin", "activate"), "w") as f:
            f.write(f'export VIRTUAL_ENV="{self.venv}"\n')
        open(os.path.join(self.site, "left_from_before"), "w").close()
        self.write_marker(self.venv, marker)

    def make_vendor(self, marker):
        os.makedirs(os.path.join(self.vendor, "law"))
        open(os.path.join(self.vendor, "left_from_before"), "w").close()
        self.write_marker(self.vendor, marker)

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

    def area_tree(self):
        """tree() but for soft/bin, which every source of env.sh writes."""
        return [p for p in self.tree() if p.split(os.sep)[0] != "bin"]

    def vendor_tree(self):
        return [p for p in self.tree() if p.split(os.sep)[0] == "vendor"]

    def marker(self, path):
        return read(os.path.join(path, ".installed"))

    def assertBuiltFromThePins(self, path, site=None):
        """`path` is marked built from the pins, and it -- or the venv's `site` -- holds every
        pinned package but pip, and nothing left from before."""
        self.assertEqual(self.marker(path), MARKER)
        path = site or path
        self.assertEqual(
            sorted(p for p in os.listdir(path) if p.endswith(".dist-info")),
            sorted(f"{pin.replace('==', '-')}.dist-info" for pin in PINS[1:]),
        )
        self.assertFalse(os.path.exists(os.path.join(path, "left_from_before")))

    def assertNothingRan(self, log):
        self.assertEqual([line for line in log if not line.startswith("rc=")], [])

    def assertSourcedAgainSucceeds(self, shell, sigint):
        proc, _ = self.run_guard(shell, "action", sigint=sigint)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("env.sh done!", proc.stdout)
        self.assertEqual(self.marker(self.venv), MARKER)
        self.assertEqual(self.marker(self.vendor), MARKER)
        self.assertEqual(sorted(os.listdir(self.soft)), ["bin", "dsprod_env", "vendor"])

    # -- building, leaving alone, rebuilding

    def test_a_fresh_area_is_built_from_the_pins(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                proc, log = self.run_guard(shell, "action")
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                pips = [line for line in log if line.startswith("pip ")]
                self.assertEqual(
                    pips[:2],
                    [
                        f"pip install --quiet --upgrade {PINS[0]}",
                        "pip install --quiet --ignore-installed " + " ".join(PINS[1:]),
                    ],
                )
                self.assertRegex(
                    pips[2],
                    rf"^pip install --quiet --target {re.escape(self.vendor)}\.tmp\.\d+ "
                    + re.escape(" ".join(PINS[1:]))
                    + "$",
                )
                self.assertEqual(len(pips), 3)
                self.assertNotIn(f"rm -rf {self.venv}", log)
                self.assertBuiltFromThePins(self.venv, self.site)
                self.assertBuiltFromThePins(self.vendor)
                self.assertEqual(log[-1], "rc=0 venv=" + self.venv)

    def test_a_second_source_leaves_what_the_first_built_alone(self):
        """The marker the build writes is the one the next source compares with."""
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                proc, _ = self.run_guard(shell, "action")
                self.assertEqual(proc.returncode, 0, proc.stderr)
                built = self.tree()
                proc, log = self.run_guard(shell, "action")
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                self.assertEqual(log, ["rc=0 venv=" + self.venv])
                self.assertEqual(self.tree(), built)

    def test_directories_built_from_the_pins_are_left_alone(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_venv(MARKER)
                self.make_vendor(MARKER)
                proc, log = self.run_guard(
                    shell, "_dsprod_driver_env", self.venv, *PINS
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                # only activated
                self.assertEqual(log, ["rc=0 venv=" + self.venv])
                proc, log = self.run_guard(shell, "_dsprod_vendor", self.vendor, *PINS)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertNothingRan(log)
                for kept in (self.site, self.vendor):
                    self.assertTrue(
                        os.path.exists(os.path.join(kept, "left_from_before"))
                    )

    def test_a_venv_that_cannot_be_activated_fails_env_sh(self):
        """A marker that matches does not make a venv usable: one whose activation fails stops
        env.sh rather than leaving law to run from whatever python is on PATH."""
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_venv(MARKER)
                self.make_vendor(MARKER)
                os.remove(os.path.join(self.venv, "bin", "activate"))
                before = self.tree()
                proc, log = self.run_guard(shell, "action")
                self.assertNotEqual(proc.returncode, 0)
                self.assertNotIn("env.sh done!", proc.stdout)
                self.assertNothingRan(log)
                self.assertEqual(self.tree(), before)

    #: markers that are not the pins: another release of a package, or of pip alone, a list
    #: recorded in another order, an empty marker (what an area built before markers carries in
    #: soft/dsprod_env), and none at all (a venv never finished; a soft/vendor built before
    #: markers)
    STALE_MARKERS = {
        "another release": OTHER,
        "another pip": " ".join(["pip==0.0.1"] + PINS[1:]) + "\n",
        "another order": " ".join([PINS[0]] + PINS[:0:-1]) + "\n",
        "empty marker": "",
        "no marker": None,
    }

    def test_a_venv_built_from_other_pins_is_rebuilt_from_scratch(self):
        for shell, case in itertools.product(self.shells, self.STALE_MARKERS):
            with self.subTest(shell=shell, case=case):
                self.fresh_area()
                self.make_venv(self.STALE_MARKERS[case])
                proc, log = self.run_guard(
                    shell, "_dsprod_driver_env", self.venv, *PINS
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("soft/dsprod_env records", proc.stdout)
                self.assertEqual(log[0], f"rm -rf {self.venv}")
                self.assertEqual(
                    log[1],
                    f"/usr/bin/python3 -m venv --system-site-packages {self.venv}",
                )
                self.assertBuiltFromThePins(self.venv, self.site)
                self.assertEqual(log[-1], "rc=0 venv=" + self.venv)

    def test_a_vendored_copy_built_from_other_pins_is_replaced_whole(self):
        for shell, case in itertools.product(self.shells, self.STALE_MARKERS):
            with self.subTest(shell=shell, case=case):
                self.fresh_area()
                self.make_vendor(self.STALE_MARKERS[case])
                proc, log = self.run_guard(shell, "_dsprod_vendor", self.vendor, *PINS)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("soft/vendor records", proc.stdout)
                self.assertBuiltFromThePins(self.vendor)
                self.assertEqual(os.listdir(self.soft), ["vendor"])
                (pip,) = [line for line in log if line.startswith("pip ")]
                building = re.fullmatch(
                    rf"pip install --quiet --target ({re.escape(self.vendor)}\.tmp\.\d+) "
                    + re.escape(" ".join(PINS[1:])),
                    pip,
                )
                self.assertIsNotNone(building, pip)
                # the old copy stays in place until pip has filled the sibling
                moved_aside = f"mv {self.vendor} {building[1]}.old"
                self.assertIn(moved_aside, log)
                self.assertLess(log.index(pip), log.index(moved_aside))

    # -- a law batch job

    def test_a_batch_job_never_builds_or_rebuilds(self):
        """HTCondor jobs source the checkout's env.sh: a pin pulled while they run must not have
        each one that starts delete the venv and the vendored copy under the others, and a job
        that starts while env.sh rebuilds them on the submitting machine must not build them
        alongside."""
        cases = [("missing", None)] + list(self.STALE_MARKERS.items())
        for shell, (case, marker), call in itertools.product(
            self.shells,
            cases,
            (("_dsprod_driver_env", self.venv), ("_dsprod_vendor", self.vendor)),
        ):
            with self.subTest(shell=shell, case=case, call=call[0]):
                self.fresh_area()
                if case != "missing":
                    (self.make_venv if call[1] == self.venv else self.make_vendor)(
                        marker
                    )
                before = self.tree()
                proc, log = self.run_guard(shell, *call, *PINS, in_law_job=True)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn(
                    f"ERROR: {call[1]} is missing, unfinished or built from other pins than "
                    "env.sh installs",
                    proc.stderr,
                )
                self.assertIn(f"env.sh installs: {' '.join(PINS)})", proc.stderr)
                self.assertIn("a batch job does not build", proc.stderr)
                self.assertIn(
                    "Source env.sh on the submitting machine and resubmit", proc.stderr
                )
                self.assertNothingRan(log)
                self.assertEqual(self.tree(), before)

    def test_a_batch_job_finding_both_built_from_the_pins_runs_as_before(self):
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_venv(MARKER)
                self.make_vendor(MARKER)
                proc, log = self.run_guard(shell, "action", in_law_job=True)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                self.assertEqual(log, ["rc=0 venv=" + self.venv])

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

    def test_a_refusal_fails_env_sh_rather_than_being_carried_past(self):
        """Without `|| return 1`, sourcing env.sh would go on to the shims and "env.sh done!"
        with an environment that was refused, and the failure would surface later and elsewhere.
        """
        for shell, (venv_marker, vendor_marker, refusing) in itertools.product(
            self.shells,
            ((OTHER, MARKER, self.venv), (MARKER, OTHER, self.vendor)),
        ):
            with self.subTest(shell=shell, refusing=os.path.basename(refusing)):
                self.fresh_area()
                self.make_venv(venv_marker)
                self.make_vendor(vendor_marker)
                proc, _ = self.run_guard(shell, "action", in_law_job=True)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn(f"ERROR: {refusing} is missing, unfinished", proc.stderr)
                self.assertIn(f"it records: {OTHER.strip()};", proc.stderr)
                self.assertNotIn("env.sh done!", proc.stdout)

    def test_a_crab_worker_neither_builds_nor_needs_a_build(self):
        """The grid branch runs the vendored copy shipped in the code tarball. Sent through the
        builds, every CRAB job -- a law batch job in an area with no soft/dsprod_env -- would stop
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

    # -- whether a directory exists

    def assertLeftAsItWas(self, proc, log, before, *errors):
        """env.sh stopped with `errors`, and nothing was built, removed or moved."""
        self.assertNotEqual(proc.returncode, 0)
        self.assertNotIn("env.sh done!", proc.stdout)
        for error in errors:
            self.assertIn(error, proc.stderr)
        self.assertNothingRan(log)
        self.assertEqual(self.tree(), before)

    def cannot_read(self, path):
        return f"ERROR: cannot read which pins {path} was built from, so it is left as it is."

    def test_a_check_answered_wrongly_once_neither_removes_nor_rebuilds(self):
        """Whether a directory exists is read once, and acted on: a check that storage answers
        wrongly once -- a stat that fails on EOS -- must not have a directory built from the pins
        taken for one never finished, nor a second check decide on the removal or replacement of
        one built from other pins. Whichever check it hits, env.sh either stops with both
        directories as they were or ends with both built from the pins."""
        injections = {
            f"{name} check {nth}": (path, {"FAKE_STAT_FAILS": f"{path}:{nth}"})
            for name, path in (("venv", self.venv), ("vendor", self.vendor))
            for nth in (1, 2, 3)
        }
        for shell, sigint, marker, case in itertools.product(
            self.shells, self.sigints, (MARKER, OTHER), injections
        ):
            target, fake = injections[case]
            with self.subTest(
                shell=shell, sigint=sigint.name, stale=marker != MARKER, case=case
            ):
                self.fresh_area()
                self.make_venv(marker if target == self.venv else MARKER)
                self.make_vendor(marker if target == self.vendor else MARKER)
                before = self.tree()
                proc, log = self.run_guard(shell, "action", sigint=sigint, **fake)
                if "FAKE_STAT_FAILS" in fake:
                    # the directory is checked at all, so the first check is answered wrongly
                    checks = read(os.path.join(self.dir, f"log.{shell}.stat"))
                    self.assertGreaterEqual(int(checks), 1)
                if proc.returncode != 0:
                    self.assertLeftAsItWas(proc, log, before, "ERROR:")
                elif marker == MARKER:
                    self.assertIn("env.sh done!", proc.stdout)
                    self.assertEqual(log, ["rc=0 venv=" + self.venv])
                    self.assertEqual(self.area_tree(), before)
                else:
                    self.assertIn("env.sh done!", proc.stdout)
                    self.assertBuiltFromThePins(
                        target, self.site if target == self.venv else None
                    )
                    self.assertEqual(self.marker(self.venv), MARKER)
                    self.assertEqual(self.marker(self.vendor), MARKER)
                    self.assertEqual(
                        sorted(os.listdir(self.soft)), ["bin", "dsprod_env", "vendor"]
                    )

    def test_a_soft_that_cannot_be_listed_is_not_taken_for_an_empty_one(self):
        """Without a listing of soft/, a directory missing there and one that the storage does not
        answer for look alike; env.sh stops rather than build or rebuild either."""
        for shell, sigint, marker in itertools.product(
            self.shells, self.sigints, (None, OTHER, MARKER)
        ):
            with self.subTest(shell=shell, sigint=sigint.name, marker=marker):
                self.fresh_area()
                if marker is not None:
                    self.make_venv(marker)
                    self.make_vendor(marker)
                before = self.tree()
                proc, log = self.run_guard(
                    shell, "action", sigint=sigint, FAKE_LS=f"fail {self.soft}"
                )
                self.assertLeftAsItWas(
                    proc,
                    log,
                    before,
                    f"ls: cannot open directory '{self.soft}'",
                    self.cannot_read(self.venv),
                )

    def test_a_listed_entry_that_is_not_a_directory_is_left_alone(self):
        for shell, sigint, name, kind in itertools.product(
            self.shells,
            self.sigints,
            ("dsprod_env", "vendor"),
            ("file", "dangling symlink"),
        ):
            with self.subTest(shell=shell, sigint=sigint.name, name=name, kind=kind):
                self.fresh_area()
                path = os.path.join(self.soft, name)
                if name == "vendor":
                    self.make_venv(MARKER)
                if kind == "file":
                    with open(path, "w") as f:
                        f.write(MARKER)
                else:
                    os.symlink(os.path.join(self.soft, "nowhere"), path)
                before = self.tree()
                proc, log = self.run_guard(shell, "action", sigint=sigint)
                self.assertLeftAsItWas(
                    proc,
                    log,
                    before,
                    f"ERROR: {path} is not a directory, or cannot be looked up.",
                    self.cannot_read(path),
                )
                self.assertEqual(os.path.islink(path), kind != "file")

    def test_an_entry_there_but_not_listed_is_left_alone(self):
        """A listing that leaves a directory out while it is there disagrees with itself, and
        neither answer is taken; a symlink is there even when what it points to is not.
        """
        states = {"the pins": MARKER, "other pins": OTHER, "dangling symlink": None}
        for shell, sigint, name, state in itertools.product(
            self.shells, self.sigints, ("dsprod_env", "vendor"), states
        ):
            with self.subTest(shell=shell, sigint=sigint.name, name=name, state=state):
                self.fresh_area()
                path = os.path.join(self.soft, name)
                make, other = (
                    (self.make_venv, self.make_vendor)
                    if name == "dsprod_env"
                    else (self.make_vendor, self.make_venv)
                )
                other(MARKER)
                if states[state] is None:
                    os.symlink(os.path.join(self.soft, "nowhere"), path)
                else:
                    make(states[state])
                before = self.tree()
                proc, log = self.run_guard(
                    shell, "action", sigint=sigint, FAKE_LS=f"omit {name}"
                )
                self.assertLeftAsItWas(
                    proc,
                    log,
                    before,
                    f"ERROR: {path} is there, but a listing of {self.soft} does not show "
                    "it.",
                    self.cannot_read(path),
                )

    def test_a_marker_there_but_not_listed_is_left_alone(self):
        """A listing of a directory that leaves its marker out while it is there disagrees with
        itself: taken for a directory without a marker, one built from the pins would be removed
        and built again. A dangling symlink is there too.
        """
        markers = {"the pins": MARKER, "other pins": OTHER, "dangling symlink": None}
        for shell, sigint, name, marker in itertools.product(
            self.shells, self.sigints, ("dsprod_env", "vendor"), markers
        ):
            with self.subTest(
                shell=shell, sigint=sigint.name, name=name, marker=marker
            ):
                self.fresh_area()
                path = os.path.join(self.soft, name)
                make, other = (
                    (self.make_venv, self.make_vendor)
                    if name == "dsprod_env"
                    else (self.make_vendor, self.make_venv)
                )
                other(MARKER)
                make(markers[marker])
                if markers[marker] is None:
                    os.symlink(
                        os.path.join(self.soft, "nowhere"),
                        os.path.join(path, ".installed"),
                    )
                before = self.tree()
                proc, log = self.run_guard(
                    shell, "action", sigint=sigint, FAKE_LS=f"omit {path}/.installed"
                )
                self.assertLeftAsItWas(
                    proc,
                    log,
                    before,
                    f"ERROR: {path}/.installed is there, but a listing of {path} does not "
                    "show it.",
                    self.cannot_read(path),
                )

    def test_an_entry_answered_absent_is_neither_built_over_nor_moved_into(self):
        """A build follows the one answer that a directory is not there; an entry there all the
        same -- left out of the listing while the lookup that checks it is answered wrongly, as
        EOS can, or made since by another source of env.sh -- stops the build: the venv is not
        built over it, nor the vendored copy moved into it.
        """
        for shell, sigint, name, marker in itertools.product(
            self.shells, self.sigints, ("dsprod_env", "vendor"), (MARKER, OTHER)
        ):
            with self.subTest(
                shell=shell, sigint=sigint.name, name=name, stale=marker != MARKER
            ):
                self.fresh_area()
                path = os.path.join(self.soft, name)
                self.make_venv(marker if name == "dsprod_env" else MARKER)
                self.make_vendor(marker if name == "vendor" else MARKER)
                before = self.tree()
                vendor_before = self.vendor_tree()
                proc, log = self.run_guard(
                    shell,
                    "action",
                    sigint=sigint,
                    FAKE_LS=f"omit {name}",
                    FAKE_STAT_FAILS=f"{path}:1",
                )
                # the listing left it out, and of the two lookups that check it the first was
                # answered wrongly
                checks = read(os.path.join(self.dir, f"log.{shell}.stat"))
                self.assertEqual(checks, "2")
                if name == "dsprod_env":
                    self.assertLeftAsItWas(
                        proc,
                        log,
                        before,
                        f"ERROR: cannot create the directory {path} for the venv; anything "
                        "there is left as it is.",
                    )
                    continue
                self.assertNotEqual(proc.returncode, 0)
                self.assertNotIn("env.sh done!", proc.stdout)
                self.assertIn(
                    f"ERROR: cannot move the new vendored copy into {path}; anything there "
                    "is left as it is.",
                    proc.stderr,
                )
                # soft/vendor is as it was, and the new copy, complete, next to it
                self.assertEqual(self.vendor_tree(), vendor_before)
                (sibling,) = set(os.listdir(self.soft)) - {"dsprod_env", "vendor"}
                self.assertRegex(sibling, r"^vendor\.tmp\.\d+$")
                self.assertBuiltFromThePins(os.path.join(self.soft, sibling))

    def test_a_checkout_without_soft_is_built_from_the_pins(self):
        """soft/ is not in git, so a fresh clone has none to list."""
        for shell in self.shells:
            with self.subTest(shell=shell):
                shutil.rmtree(self.soft, ignore_errors=True)
                proc, log = self.run_guard(shell, "action")
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                self.assertBuiltFromThePins(self.venv, self.site)
                self.assertBuiltFromThePins(self.vendor)

    def test_a_soft_that_is_not_a_directory_stops_env_sh_at_its_creation(self):
        """soft/ is created when missing, so that it can be listed; a file or a dangling symlink
        in its place stops env.sh there, before anything is read from it or built under it.
        """
        for shell, sigint, kind in itertools.product(
            self.shells, self.sigints, ("file", "dangling symlink")
        ):
            with self.subTest(shell=shell, sigint=sigint.name, kind=kind):
                if os.path.islink(self.soft) or os.path.isfile(self.soft):
                    os.remove(self.soft)
                else:
                    shutil.rmtree(self.soft)
                nowhere = os.path.join(self.dir, "nowhere")
                if kind == "file":
                    with open(self.soft, "w") as f:
                        f.write(MARKER)
                else:
                    os.symlink(nowhere, self.soft)
                before = set(os.listdir(self.dir))
                proc, log = self.run_guard(shell, "action", sigint=sigint, LC_ALL="C")
                self.assertNotEqual(proc.returncode, 0)
                self.assertNotIn("env.sh done!", proc.stdout)
                # mkdir's own error is all that is printed
                self.assertEqual(proc.stdout, "")
                self.assertEqual(
                    proc.stderr,
                    f"mkdir: cannot create directory '{self.soft}': File exists\n",
                )
                self.assertNothingRan(log)
                self.assertLessEqual(
                    set(os.listdir(self.dir)), before | {"data", f"log.{shell}"}
                )
                self.assertFalse(os.path.lexists(nowhere))
                if kind == "file":
                    self.assertEqual(read(self.soft), MARKER)
                else:
                    self.assertEqual(os.readlink(self.soft), nowhere)

    def test_a_quoting_style_exported_by_the_sourcing_shell_hides_nothing(self):
        """GNU ls quotes every name it lists, into a pipe too, when QUOTING_STYLE says so; a
        sourcing shell that exports it must not hide a directory or its marker from env.sh.
        """
        for shell, style in itertools.product(self.shells, ("shell-always", "c")):
            with self.subTest(shell=shell, style=style):
                self.fresh_area()
                self.make_venv(MARKER)
                self.make_vendor(MARKER)
                before = self.tree()
                proc, log = self.run_guard(shell, "action", QUOTING_STYLE=style)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                self.assertEqual(log, ["rc=0 venv=" + self.venv])
                self.assertEqual(self.area_tree(), before)

    # -- a marker that cannot be read

    @unittest.skipIf(os.geteuid() == 0, "root reads any file")
    def test_a_marker_that_cannot_be_read_is_not_guessed_at(self):
        """Storage that does not answer is not a stale build: env.sh stops, and neither removes
        nor rebuilds the directory -- outside a batch job too."""
        cases = {
            # what cannot be read: (the directory, the path made unreadable)
            "venv marker": (self.venv, os.path.join(self.venv, ".installed")),
            "venv listing": (self.venv, self.venv),
            "vendor marker": (self.vendor, os.path.join(self.vendor, ".installed")),
            "vendor listing": (self.vendor, self.vendor),
        }
        for shell, case in itertools.product(self.shells, cases):
            with self.subTest(shell=shell, case=case):
                refusing, unreadable = cases[case]
                self.fresh_area()
                self.make_venv(OTHER if refusing == self.venv else MARKER)
                self.make_vendor(OTHER)
                before = self.tree()
                os.chmod(unreadable, 0)
                try:
                    proc, log = self.run_guard(shell, "action")
                finally:
                    os.chmod(unreadable, 0o755)
                self.assertNotEqual(proc.returncode, 0)
                self.assertNotIn("env.sh done!", proc.stdout)
                self.assertIn(
                    f"ERROR: cannot read which pins {refusing} was built from, so it is left "
                    "as it is.",
                    proc.stderr,
                )
                self.assertNothingRan(log)
                self.assertEqual(self.tree(), before)

    def test_aliases_of_the_sourcing_shell_do_not_change_what_is_read(self):
        """env.sh is sourced into interactive shells, whose aliases apply to the functions it
        defines; an aliased `ls`, `grep` or `cat` must not turn into a refusal or a rebuild.
        """
        for shell in self.shells:
            with self.subTest(shell=shell):
                self.fresh_area()
                self.make_venv(MARKER)
                self.make_vendor(MARKER)
                before = self.tree()
                proc, log = self.run_guard(
                    shell, "_dsprod_driver_env", self.venv, *PINS, FAKE_ALIASES="1"
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(log, ["rc=0 venv=" + self.venv])
                proc, log = self.run_guard(
                    shell, "_dsprod_vendor", self.vendor, *PINS, FAKE_ALIASES="1"
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertNothingRan(log)
                self.assertEqual(self.tree(), before)

    def test_the_ifs_of_the_sourcing_shell_does_not_change_the_pins_compared(self):
        """The pins are joined with spaces for the marker whatever IFS the sourcing shell has
        set: one splitting on newlines only must not see an area built from the pins as built
        from others -- a refusal in a batch job, a rebuild of both directories on the
        submitting machine -- and that IFS is the shell's again once env.sh is sourced.
        """
        for shell, in_law_job in itertools.product(self.shells, (False, True)):
            with self.subTest(shell=shell, in_law_job=in_law_job):
                self.fresh_area()
                proc, _ = self.run_guard(shell, "action")
                self.assertEqual(proc.returncode, 0, proc.stderr)
                built = self.tree()
                proc, log = self.run_guard(
                    shell, "action", in_law_job=in_law_job, CALLER_IFS="\n"
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                self.assertIn("IFS after the call: [\n]\n", proc.stdout)
                self.assertEqual(log, ["rc=0 venv=" + self.venv])
                self.assertEqual(self.tree(), built)
                self.assertEqual(self.marker(self.venv), MARKER)
                self.assertEqual(self.marker(self.vendor), MARKER)

    def test_a_build_under_another_ifs_records_the_pins_space_separated(self):
        """A marker written with the sourcing shell's IFS would be seen as other pins by every
        shell with the default one, which would rebuild both directories again."""
        for shell, old in itertools.product(self.shells, (None, OTHER)):
            with self.subTest(shell=shell, old=old):
                self.fresh_area()
                if old:
                    self.make_venv(old)
                    self.make_vendor(old)
                proc, _ = self.run_guard(shell, "action", CALLER_IFS="\n")
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                self.assertIn("IFS after the call: [\n]\n", proc.stdout)
                self.assertBuiltFromThePins(self.venv, self.site)
                self.assertBuiltFromThePins(self.vendor)
                built = self.tree()
                proc, log = self.run_guard(shell, "action", in_law_job=True)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(log, ["rc=0 venv=" + self.venv])
                self.assertEqual(self.tree(), built)

    # -- a build step that fails

    def test_a_vendoring_cut_short_leaves_nothing_half_filled_in_place(self):
        """A half-filled copy must never stand where soft/vendor is read, since the CRAB code
        tarball is made from it; nor may a pip that failed leave soft/vendor missing, which the
        tarball would silently go without. Sourcing env.sh stops at the failure whether the shell
        takes SIGINT or not.
        """
        for shell, sigint, fails, old in itertools.product(
            self.shells, self.sigints, ("pip", "pip_killed"), (None, OTHER)
        ):
            with self.subTest(shell=shell, sigint=sigint.name, fails=fails, old=old):
                self.fresh_area()
                self.make_venv(MARKER)
                if old:
                    self.make_vendor(old)
                before = self.tree()
                proc, _ = self.run_guard(
                    shell, "action", sigint=sigint, FAKE_FAILS=fails
                )
                self.assertNotEqual(proc.returncode, 0)
                self.assertNotIn("env.sh done!", proc.stdout)
                self.assertIn(
                    "ERROR: vendoring the pinned packages failed; "
                    f"{self.vendor} is left as it was.",
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
                self.assertSourcedAgainSucceeds(shell, sigint)
                self.assertBuiltFromThePins(self.vendor)

    def test_what_an_unfinished_vendoring_left_is_removed_through_a_symlinked_soft(
        self,
    ):
        """soft/ may be a symlink to the storage that holds the area. What an unfinished vendoring
        left next to soft/vendor is removed whichever soft/ is -- kept, a leftover whose name a
        later build reuses would be filled on top of what it holds -- and nothing further down
        is."""
        real = os.path.join(self.dir, "real_soft")
        for shell, kind, old in itertools.product(
            self.shells, ("directory", "symlink"), (None, OTHER)
        ):
            with self.subTest(shell=shell, kind=kind, old=old):
                for path in (self.soft, real):
                    if os.path.islink(path):
                        os.remove(path)
                    elif os.path.isdir(path):
                        shutil.rmtree(path)
                if kind == "symlink":
                    os.makedirs(real)
                    os.symlink(real, self.soft)
                else:
                    os.makedirs(self.soft)
                self.make_venv(MARKER)
                if old:
                    self.make_vendor(old)
                os.makedirs(os.path.join(self.soft, "vendor.tmp.1", "law"))
                nested = os.path.join(self.soft, "x", "vendor.tmp.2", "law")
                os.makedirs(nested)
                proc, _ = self.run_guard(shell, "action")
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertIn("env.sh done!", proc.stdout)
                self.assertBuiltFromThePins(self.vendor)
                self.assertEqual(
                    sorted(os.listdir(self.soft)), ["bin", "dsprod_env", "vendor", "x"]
                )
                self.assertTrue(os.path.isdir(nested))
                self.assertEqual(os.path.islink(self.soft), kind == "symlink")

    def test_a_vendoring_step_that_fails_stops_env_sh(self):
        """Removing what an earlier vendoring left, marking the new copy and the two renames are
        checked as pip is. In a shell that ignores SIGINT, `run_cmd` would carry on: into pip next
        to a leftover it could not remove, into moving an unmarked copy into place, into renaming
        the new copy into the old one it could not move aside, or into deleting the old copy with
        no new one in its place -- and end in `env.sh done!`.
        """
        errors = {
            "stale_removal": "ERROR: cannot remove what an unfinished vendoring left next to "
            f"{self.vendor}; {self.vendor} is left as it was.",
            "marker": "ERROR: cannot mark the new vendored copy finished; "
            f"{self.vendor} is left as it was.",
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
                self.make_venv(MARKER)
                self.make_vendor(OTHER)
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
                if fails == "marker":
                    self.assertFalse([line for line in log if line.startswith("mv ")])
                if fails == "mv_into_place":
                    # the old copy is aside and the new one, complete, next to it
                    self.assertFalse(os.path.exists(self.vendor))
                    new, old = siblings
                    self.assertRegex(new, r"^vendor\.tmp\.\d+$")
                    self.assertEqual(old, f"{new}.old")
                    self.assertBuiltFromThePins(os.path.join(self.soft, new))
                    self.assertTrue(
                        os.path.exists(os.path.join(self.soft, old, "left_from_before"))
                    )
                else:
                    self.assertEqual(self.vendor_tree(), before)
                self.assertSourcedAgainSucceeds(shell, sigint)
                self.assertBuiltFromThePins(self.vendor)

    def test_a_venv_build_step_that_fails_stops_env_sh(self):
        """The venv is marked only once everything is in it, and sourcing env.sh stops at the
        step that failed whether the shell takes SIGINT or not. In a shell that ignores it,
        `run_cmd` would carry on: into activating the old venv it could not remove, into pip
        outside a venv that was never made or could not be activated, or past an install that
        failed into marking the venv built from the pins -- and end in `env.sh done!`.
        """
        cases = {
            # failing step: (marker of the venv before, error, last step run)
            "rm": (OTHER, f"ERROR: cannot remove {self.venv} to rebuild it.", "rm"),
            "venv": (
                None,
                f"ERROR: creating the venv {self.venv} failed;",
                "/usr/bin/python3",
            ),
            "activate": (
                None,
                f"ERROR: activating the new venv {self.venv} failed;",
                "/usr/bin/python3",
            ),
            "pip_upgrade": (
                OTHER,
                f"ERROR: installing {PINS[0]} into {self.venv} failed;",
                "pip",
            ),
            "pip": (
                OTHER,
                f"ERROR: installing the pinned packages into {self.venv} failed;",
                "pip",
            ),
            "marker": (None, f"ERROR: cannot mark {self.venv} finished;", "pip"),
        }
        for shell, sigint, fails in itertools.product(self.shells, self.sigints, cases):
            with self.subTest(shell=shell, sigint=sigint.name, fails=fails):
                marker_before, error, last = cases[fails]
                self.fresh_area()
                if marker_before is not None:
                    self.make_venv(marker_before)
                self.make_vendor(MARKER)
                before = self.tree()
                proc, log = self.run_guard(
                    shell, "action", sigint=sigint, FAKE_FAILS=fails
                )
                self.assertNotEqual(proc.returncode, 0)
                self.assertNotIn("env.sh done!", proc.stdout)
                self.assertIn(error, proc.stderr)
                ran = [line.split()[0] for line in log if not line.startswith("rc=")]
                self.assertEqual(ran[-1], last)
                if fails == "rm":
                    self.assertEqual(ran, ["rm"])
                    self.assertEqual(self.tree(), before)
                else:
                    self.assertFalse(
                        os.path.isfile(os.path.join(self.venv, ".installed"))
                    )
                if fails == "marker":
                    # what stands where the marker goes cannot be read, so the next source
                    # leaves the venv alone until it is removed by hand
                    proc, log = self.run_guard(shell, "action", sigint=sigint)
                    self.assertNotEqual(proc.returncode, 0)
                    self.assertIn(f"cannot read which pins {self.venv}", proc.stderr)
                    self.assertNothingRan(log)
                    shutil.rmtree(self.venv)
                # the next source builds it
                self.assertSourcedAgainSucceeds(shell, sigint)
                self.assertBuiltFromThePins(self.venv, self.site)


if __name__ == "__main__":
    unittest.main()
