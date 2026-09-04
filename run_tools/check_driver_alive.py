#!/usr/bin/env python3
"""Alarm when nothing is driving a CRAB production any more.

A dead driver is invisible from the outside: the CRAB tasks keep running, jobs keep finishing, and
the production simply stops making progress -- nothing is polled, nothing that failed is
resubmitted, and nothing downstream is merged. In the Run3_2023BPix production one such gap
(nothing polled between 09-01 16:20 and 09-03 14:12) was 27 h of the 68.4 h that production took to
reach 99.4 %, and `data/jobs/` records 14 driver invocations over 7 days.

The liveness signal is the newest `data/*/*/crab_jobs_*.json`: law rewrites that dump on every poll
iteration, so its mtime is visible from any machine that can read the production area -- including
the one the driver did *not* die on.

Nothing here imports law or dsprod, so it runs under a bare python3 from acron. `--help` shows the
acron line.
"""

import argparse
import glob
import json
import os
import sys
import time
from collections import namedtuple

#: measured poll gaps in the live production are p50 5.3 min, p90 12.1 min and max 32.3 min --
#: submission is silent for 5-13 min at a time and a failed status query skips the dump entirely --
#: so a threshold derived from poll_interval (5 min) alarms on a healthy production
DEFAULT_THRESHOLD_MINUTES = 45.0

#: law's job-data dump: data/<Task>/<store dir>/crab_jobs_<branch range>.json
JOB_DUMP_GLOB = os.path.join("data", "*", "*", "crab_jobs_*.json")

#: CRAB project directories, created next to the job files of the submitting law process
PROJECT_GLOB = os.path.join("data", "jobs", "*", "crab_*")

#: where the stall last reported is remembered, next to the driver logs
STATE_PATH = os.path.join("data", "logs", "driver_alarm_state.json")

#: a job in one of these states leaves a restarted driver nothing to do. `failed` is deliberately
#: not one of them: law rebuilds `_job_retries` empty in every process and persists only
#: `job_data.attempts`, which nothing reads back (law/workflow/remote.py), so a restart hands each
#: failed branch a full retry budget and resubmits it after one polling iteration. Five fast
#: failures inside one leg is what a black-hole site produces, and it happens at the tail, which is
#: exactly where a stall costs a production its last percent.
NO_WORK_STATUSES = ("finished",)

DEFAULT_AREA = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

Observation = namedtuple(
    "Observation", ["area", "projects", "dump", "mtime", "work_left"]
)


def crab_project_dirs(area, glob_fn=glob.glob):
    """CRAB project directories under *area*: the evidence that it ever submitted to the grid."""
    return sorted(
        p for p in glob_fn(os.path.join(area, PROJECT_GLOB)) if os.path.isdir(p)
    )


def newest_job_dump(area, glob_fn=glob.glob):
    """The most recently written law job-data dump under *area*, as (path, mtime).

    Globbed rather than named: the file carries the workflow's branch range
    (`crab_jobs_0To4800.json`) and there is one per store directory, so which file law is currently
    rewriting is not knowable in advance.
    """
    newest, newest_mtime = None, None
    for path in glob_fn(os.path.join(area, JOB_DUMP_GLOB)):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if newest_mtime is None or mtime > newest_mtime:
            newest, newest_mtime = path, mtime
    return newest, newest_mtime


def work_left(path):
    """Whether the dump still lists jobs to drive; None when it cannot be read.

    law rewrites the dump in place on every poll, so a read can catch it truncated or half-written.
    That is a sign of a driver at work, not of a broken production, and must not raise here.
    """
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("unsubmitted_jobs"):
        return True
    jobs = data.get("jobs")
    if not isinstance(jobs, dict):
        return None
    for entry in jobs.values():
        if not isinstance(entry, dict):
            return None
        if entry.get("status") not in NO_WORK_STATUSES:
            return True
    return False


def observe(area, glob_fn=glob.glob):
    """Read everything the decision needs from the production area."""
    dump, mtime = newest_job_dump(area, glob_fn=glob_fn)
    return Observation(
        area=area,
        projects=crab_project_dirs(area, glob_fn=glob_fn),
        dump=dump,
        mtime=mtime,
        work_left=None if dump is None else work_left(dump),
    )


def load_state(path):
    """The last stall reported, so that one stall is reported once and not every 30 minutes."""
    try:
        with open(path) as f:
            state = json.load(f)
    except (OSError, ValueError):
        return {}
    return state if isinstance(state, dict) else {}


def save_state(path, state):
    """Persist the state, returning why it could not be written, or None on success."""
    try:
        # dirname("") for a bare file name would raise, and the stall would then repeat forever
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            json.dump(state, f)
    except OSError as exc:
        # losing the state only costs the repeat suppression; it must neither raise nor print on
        # its own, or an unwritable area would mail the operator from the healthy path
        return f"could not write {path}: {exc}"
    return None


def fmt_age(seconds):
    seconds = int(max(seconds, 0))
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


def stall_message(obs, age, threshold_seconds):
    """The report for a production that nothing has polled in *age* seconds."""
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(obs.mtime))
    return "\n".join(
        [
            f"DSProd: no driver has polled for {fmt_age(age)} "
            f"(threshold {fmt_age(threshold_seconds)})",
            f"  area:       {obs.area}",
            f"  last poll:  {stamp}  {os.path.relpath(obs.dump, obs.area)}",
            f"  crab tasks: {len(obs.projects)} project directories under "
            f"{os.path.join('data', 'jobs')}",
            "Jobs already submitted keep running, but nothing polls them, resubmits what failed,",
            "or merges what finished. Restart the driver on a machine that can reach the area:",
            f"  cd {obs.area} && source env.sh && run_tools/drive.sh law run <task> "
            "--setup <setup>.yaml --workflow crab",
        ]
    )


def decide(obs, now, threshold_seconds, state):
    """Return (message to report, new state). *message* is None when there is nothing to say."""
    if not obs.projects:
        # an area that never submitted to CRAB (a fresh checkout, a local-only production) has no
        # driver to miss -- alarming there would train the operator to ignore the mail
        return None, {}
    if obs.dump is None:
        # law never removes its job directories (job_file_dir_cleanup: False in config/law.cfg), so
        # an area whose store directories were cleared to restart clean stays in this state for good
        message = (
            f"DSProd: {len(obs.projects)} CRAB project directories under {obs.area}, but no "
            f"{JOB_DUMP_GLOB} was ever written -- no driver has recorded a poll."
        )
        new_state = {"dump": "", "mtime": 0}
    elif obs.work_left is False:
        # nothing is left for a driver to do, so the last dump only gets older from here
        return None, {}
    elif now - obs.mtime <= threshold_seconds:
        return None, {}
    else:
        message = stall_message(obs, now - obs.mtime, threshold_seconds)
        new_state = {"dump": obs.dump, "mtime": obs.mtime}

    # every report leaves through here: acron runs this every 30 minutes, and a stall changes
    # nothing that could be keyed on, so the same mail 48 times a day would stop being read
    if (
        state.get("dump") == new_state["dump"]
        and state.get("mtime") == new_state["mtime"]
    ):
        return None, state
    return message, new_state


def acron_hint():
    script = os.path.abspath(__file__)
    return (
        "acron, so that the alarm does not live on the machine whose death it reports:\n"
        "\n"
        "  acrontab -e\n"
        f"  */30 * * * * lxplus.cern.ch {script} --area {DEFAULT_AREA}\n"
        "\n"
        "Point --area at the production area if it is not this checkout. Nothing is printed while\n"
        "a driver is polling, so acron mails only when a stall starts."
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        epilog=acron_hint(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--area", default=DEFAULT_AREA, help="production area (default: %(default)s)"
    )
    parser.add_argument(
        "--threshold-minutes",
        type=float,
        default=DEFAULT_THRESHOLD_MINUTES,
        help="alarm when the newest poll is older than this (default: %(default)s)",
    )
    parser.add_argument(
        "--state-file",
        default=None,
        help=f"where the last reported stall is remembered (default: <area>/{STATE_PATH})",
    )
    args = parser.parse_args(argv)

    area = os.path.abspath(args.area)
    state_file = args.state_file or os.path.join(area, STATE_PATH)
    previous = load_state(state_file)
    message, state = decide(
        observe(area), time.time(), args.threshold_minutes * 60.0, previous
    )
    problem = save_state(state_file, state) if state != previous else None
    if message is None:
        return 0
    if problem:
        message += f"\n(this report may repeat: {problem})"
    print(message, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
