"""CRAB backend for DSProd law tasks (more grid resources than local HTCondor).

Built on ``law.contrib.cms.CrabWorkflow``, modelled on FLAF PR #299 but much simpler:
CRAB is only the batch backend — DSProd writes all products (gridpacks, nano) to EOS via
law WLCG targets, so CRAB's own stageout/log transfer is forced off. WLCG workers have no
AFS, so the DSProd code + genproductions_scripts are shipped as a CRAB ``inputFiles`` tarball
(built at submit time) and unpacked by ``bootstrap.sh``; CMSSW is set up on the worker from
cvmfs on demand (our releases are standard central releases).

There is **no CRAB-specific output location**: products always go to ``fs_default``, whatever the
backend. Only the compute knobs are configurable, in the merged global config
(``config/global.yaml`` + ``user_custom.yaml``), never in a production setup::

    crab:
      max_cores: 4             # 1, 2, 4 or 8; caps a task's cores AND what its memory may need
      # memory_mb: 0           # optional default request in MB; 0 = max(3000, 2500 * numCores)
      # whitelist: [ ... ]       # optional; default = every tier (T1_*, T2_*, T3_*)
      # blacklist: [ ... ]       # optional; exclude sites that fail to reach the storage
      # parallel_jobs: 5000      # jobs per CRAB task / in flight; --parallel-jobs wins
      #                          # `auto` scales it with the branch count (see auto_parallel_jobs)
      # refill_fraction: 0.2     # min wave size / free slots, as a fraction of parallel_jobs
      # retry_release_minutes: 45  # a parked retry goes out after this long, whatever the wave
"""

import contextlib
import fnmatch
import json
import math
import os
import re
import subprocess
import threading
import time
import urllib.request
import uuid
from collections import Counter, OrderedDict

import law
import luigi
from law.job.base import JobInputFile

from .site_stats import SiteStats
from .tools import (
    ResyncExistingBranchesProxy,
    StopOnMassInitialRetryProxy,
    timed_call_wrapper,
    update_kerberos_ticket,
)
from .watchdog import Heartbeat, StallWatchdog, watchdog_config

law.contrib.load("cms")


# law builds the paths of the files it ships with every CRAB submission -- the job wrapper and
# its PSet -- as `rel_path(__file__, ...)`, and `law.util.rel_path` strips the file name from the
# anchor only when `os.path.exists()` confirms it is a file. Our software tree lives on EOS, and
# one transient stat failure there was enough: `rel_path` treated `law/contrib/cms/job.py` as a
# *directory*, and the submission died copying `.../job.py/crab/crab_wrapper.sh`, taking a
# multi-day production with it. A module file is never a directory, whether or not storage answers
# right now, so anchor resolution must not depend on a stat succeeding.
def _strict_rel_path(anchor, *paths):
    anchor = os.path.abspath(os.path.expandvars(os.path.expanduser(str(anchor))))
    if anchor.endswith(".py") or os.path.isfile(anchor):
        anchor = os.path.dirname(anchor)
    return os.path.normpath(os.path.join(anchor, *map(str, paths)))


law.contrib.cms.job.rel_path = _strict_rel_path

#: site CRAB is told to stage out to. Never actually written to (stageout is disabled), but the
#: submit-time check requires a site the user can write to; CERNBOX is the CERN-account default.
_CRAB_DUMMY_SITE = "T3_CH_CERNBOX"

#: Site.whitelist used when the config sets none. It is NOT optional: DSProd jobs have no input
#: dataset, so the config sets `Data.ignoreLocality`, and the CRAB client then refuses to submit
#: without a whitelist ("when ignoreLocality is set a valid site white list must be specified",
#: CRABClient/Commands/submit.py). Listing every tier is the widest pool the client accepts.
_CRAB_ALL_SITES = ("T1_*", "T2_*", "T3_*")

#: jobs per CRAB task, and the number law keeps in flight. A CRAB task tops out around 10k jobs,
#: so a large production (tens of thousands of branches) must be split into waves rather than
#: submitted as one task.
_CRAB_DEFAULT_PARALLEL_JOBS = 5000

#: `parallel_jobs` value that asks for the size of the production to decide it
_CRAB_AUTO_PARALLEL_JOBS = "auto"

#: ceiling for `parallel_jobs: auto`. The "a CRAB task holds ~10k jobs" figure is folklore: the
#: limit is enforced on the server and invisible from the client, and the largest task submitted
#: from here so far held 4090 jobs. 8000 stays clearly below the figure everyone quotes.
_CRAB_AUTO_MAX_PARALLEL_JOBS = 8000

#: minimum size of a wave, as a fraction of `parallel_jobs`, and the number of slots that must be
#: free to take it. Without it law submits a fresh CRAB task as soon as a single job finishes or
#: fails, producing hundreds of tiny tasks.
_CRAB_DEFAULT_REFILL_FRACTION = 0.2

#: how long a retry held back by the wave gate may wait before it goes out on its own, whatever
#: the wave size. Waiting for a wave that a handful of retries cannot fill costs a full job length
#: (7.1 h at the median) per retry generation: over a 4800-job production the parked retries
#: waited 11.35 h at the median, ~10.5 h of the 68.4 h it took to reach 99.4 %.
_CRAB_DEFAULT_RETRY_RELEASE_MINUTES = 45

#: CRAB's own resource limits, from ServerUtilities.MAX_MEMORY_PER_CORE / MAX_MEMORY_SINGLE_CORE.
#: The client refuses a task above max(MAX_MEMORY_SINGLE_CORE, numCores * MAX_MEMORY_PER_CORE), so
#: memory can only ever be asked for *downward* -- more than 2500 MB per core is bought with cores.
CRAB_MB_PER_CORE = 2500
CRAB_MB_SINGLE_CORE = 3000

#: the only values `JobType.numCores` accepts (CRABClient/JobType/CMSSWConfig.py); anything else is
#: refused at submit, which a computed core count can otherwise walk straight into
CRAB_ALLOWED_CORES = (1, 2, 4, 8)


def _crab_cores_up(n):
    """The smallest core count CRAB accepts that is at least `n`."""
    return next((c for c in CRAB_ALLOWED_CORES if c >= n), CRAB_ALLOWED_CORES[-1])


def _crab_cores_down(n):
    """The largest core count CRAB accepts that is at most `n`."""
    return next(
        (c for c in reversed(CRAB_ALLOWED_CORES) if c <= n), CRAB_ALLOWED_CORES[0]
    )


def auto_parallel_jobs(
    n_branches,
    default=_CRAB_DEFAULT_PARALLEL_JOBS,
    cap=_CRAB_AUTO_MAX_PARALLEL_JOBS,
):
    """Jobs to keep in flight for a production of `n_branches` branches (`parallel_jobs: auto`).

    Nothing here queues -- 4798 of the 4800 branches of the Run3_2023BPix production this was
    measured on started within half an hour of being submitted -- so the makespan is the ramp plus
    one job length per *wave*, and the number of waves is `n_branches / parallel_jobs`. Raising
    the ceiling is therefore the only lever on a production too large to go out at once: at the
    288000 branches of the 2024 setup, 8000 in flight instead of 5000 is 36 waves instead of 57.6,
    ~-37 % of the makespan.

    Never *below* the fixed default, so a production that already fits in one wave is submitted
    exactly as before. That is a floor, not a description of any particular era: the 40-mass grid
    at both spins puts Run3_2023BPix at 16000 branches, well past the default, so it now takes the
    cap too -- it was 4800 when this was measured.
    """
    return max(1, min(int(cap), max(int(default), int(n_branches))))


def build_code_tarball(ana_path, out_path):
    """Tar the DSProd code needed on a WLCG worker (no AFS there).

    Deliberately **without** `gridpacks`: a CRAB input sandbox is size-limited, and a job that
    needs a gridpack downloads it from `fs_default` (where `MakeGridpack` put it) at run time.
    """
    includes = [
        "dsprod",
        "models",  # model plugins + cards + fragments (DSProdModels submodule)
        "config",
        "env.sh",
        "bootstrap.sh",
        "genproductions_scripts",
        # vendored pure-python law + luigi (+ deps), used by env.sh on grid workers where
        # there is no PyPI access and the system python is too old to pip-install luigi.
        "soft/vendor",
    ]
    present = [p for p in includes if os.path.exists(os.path.join(ana_path, p))]

    # Build next to the destination and rename, so a failed build never leaves a truncated
    # tarball where the next submission (or `bootstrap.sh`, which globs `dsprod_code*.tar.gz`)
    # would pick it up.
    tmp_path = f"{out_path}.tmp"
    proc = subprocess.run(
        [
            "tar",
            "-czf",
            tmp_path,
            "--warning=no-file-changed",
            "--exclude=__pycache__",
            "--exclude=.git",
            *present,
        ],
        cwd=ana_path,
        capture_output=True,
        text=True,
    )
    # GNU tar exits 1 when a file or directory changed while it was being read -- routine on an
    # EOS-mounted production area, and harmless: the entry is still archived in full. Failing here
    # aborts the submission of a whole production, so verify the archive instead of trusting the
    # exit code. 2 and above are real errors.
    if proc.returncode >= 2:
        _remove_quietly(tmp_path)
        raise RuntimeError(
            f"could not build the CRAB code tarball (tar exit {proc.returncode}):\n"
            f"{proc.stderr.strip()}"
        )
    try:
        _verify_code_tarball(tmp_path, present)
    except Exception:
        _remove_quietly(tmp_path)
        raise
    os.replace(tmp_path, out_path)
    return out_path


def _remove_quietly(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _verify_code_tarball(path, expected):
    """Raise unless the archive is readable and holds every requested top-level entry."""
    proc = subprocess.run(["tar", "-tzf", path], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"the CRAB code tarball {path} is not readable:\n{proc.stderr.strip()}"
        )
    top = {name.split("/", 1)[0] for name in proc.stdout.split("\n") if name}
    missing = [e for e in expected if e.split("/", 1)[0] not in top]
    if missing:
        raise RuntimeError(
            f"the CRAB code tarball {path} is incomplete, missing: {', '.join(missing)}"
        )


class CrabTaskRefused(Exception):
    """A task the CRAB server will never run, carrying the reason it gave.

    Raised instead of law's generic unreadable-status error so that `query` can tell a dead task
    from a slow one without re-parsing the response.
    """

    def __init__(self, state, warnings, proj_dir):
        self.state = state
        self.warnings = list(warnings or [])
        self.proj_dir = proj_dir
        reason = "; ".join(self.warnings) or "no reason given by the server"
        super(CrabTaskRefused, self).__init__(
            f"the CRAB server refused {os.path.basename(str(proj_dir))} ({state}): {reason}"
        )


class CrabTaskNotScheduledYet(Exception):
    """A task the CRAB server has accepted but not yet handed to a scheduler."""

    def __init__(self, state):
        self.state = state
        super(CrabTaskNotScheduledYet, self).__init__(
            f"the task is {state}: accepted by the CRAB server, not yet on a scheduler"
        )


class DSProdCrabJobManager(law.cms.CrabJobManager):
    """CRAB job manager that rides out a status response it cannot read.

    `crab status` occasionally returns output with no "Status on the CRAB server" line at all. law
    then raises, and because `query_group` maps a group failure onto every job of the task, one such
    response became **4763 identical errors** in a single production poll. Worse, law `continue`s the
    whole poll iteration on any query error: no status line, no resubmission, and the other task's
    perfectly good data discarded with it -- and `poll_fails` consecutive occurrences kill the
    workflow.

    The condition is transient, so the query is simply retried. If it still cannot be read, the
    task's jobs are reported as pending -- what law itself does when a freshly submitted task has no
    per-job information yet -- and the fact is published once, for the task, instead of once per job.
    A task that stays unreadable for `max_unreadable_polls` consecutive polls does raise: a
    production that quietly stalls is worse than one that stops.
    """

    #: attempts, and the pause between them, before a status response is given up on
    query_retries = 3
    query_retry_delay = 15.0

    #: consecutive unreadable polls of one task that are tolerated before raising
    max_unreadable_polls = 10

    def __init__(self, *args, **kwargs):
        super(DSProdCrabJobManager, self).__init__(*args, **kwargs)
        #: proj_dir -> number of consecutive polls whose response could not be read
        self._unreadable = {}
        #: keys already reported, so a status that repeats every poll is printed once
        self._noted = set()
        #: project dirs the server refused, counted once each: polling one refused task ten times
        #: is one refused submission, not ten
        self._refused_projects = set()
        #: proj_dir -> consecutive polls the task has been accepted but not scheduled
        self._unscheduled = {}
        #: why the run must stop, read and raised by the poll callback. Raising from `query` does
        #: not work: law runs it in a thread pool and `get_async_result_silent` turns an exception
        #: into the *result*, so the raise would be counted as one more unreadable poll while the
        #: refused task's jobs stayed unfailed and every other project lost that poll's status.
        self.stop_reason = None
        #: job ids already recorded, and the in-flight counts per project
        self._stats_seen = set()
        self._stats_lock = threading.Lock()
        self._in_flight = {}

    #: server statuses of a task that will never produce a job. `SUBMITREFUSED` is set by the CRAB
    #: TaskWorker when it rejects the request outright -- an unknown site name in the whitelist, say
    #: -- and it is absorbing: the TaskWorker only ever picks up `HOLDING`, and `crab resubmit` and
    #: even `crab kill` refuse a task in it. Polling it again can only repeat, so it is reported as
    #: a failure of its jobs instead, which law retries into a fresh task. `SUBMITFAILED` is the
    #: An explicit list, never "whatever law will not accept": law 0.1.20 does not accept
    #: `WAITING` either, and that is every task's first status. `SUBMITFAILED` is deliberately
    #: absent -- that is the TaskWorker or the schedd failing rather than refusing, i.e. the
    #: transient class, which the retry path already handles.
    terminal_server_states = ("SUBMITREFUSED",)

    #: statuses meaning "accepted, but not on a scheduler yet" that law 0.1.20 does not know. Every
    #: task now enters the CRAB database as `WAITING` (`CRABInterface/DataWorkflow.py`) and is
    #: promoted later, so without this a perfectly healthy submission spends `query_retries` x
    #: `query_retry_delay` seconds being retried and counts against `max_unreadable_polls` -- and a
    #: backlogged TaskWorker, i.e. several productions submitting at once, is exactly when a task
    #: lingers there.
    pending_server_states = ("WAITING",)

    #: polls a task may spend unscheduled before the run is stopped. The point of accepting the
    #: status at all is that a backlogged TaskWorker is normal, so this is deliberately generous --
    #: five hours at the default interval -- but it cannot be absent: a task that never leaves
    #: `WAITING` would otherwise be polled for ever with every job pending and nothing said.
    max_unscheduled_polls = 60

    #: how often the wait is repeated in the log while it lasts
    unscheduled_report_every = 12

    #: distinct submissions the server may refuse before the run is stopped. A refusal is a verdict
    #: on what was sent, not on the grid: the first one can be a stale site list, which is dropped
    #: here, so a second one on a freshly read list is a configuration fault. Retrying instead
    #: would spend every branch's attempts on the same verdict and end in "acceptance not reached".
    max_refused_submissions = 2

    @classmethod
    def server_status(cls, out):
        """The `Status on the CRAB server` value, matched the way law matches it.

        `query_server_status_cre` is anchored `^...$` and compiled without `re.MULTILINE`, and law
        applies it per line; searching the whole response with it returns nothing.
        """
        for line in (out or "").replace("\r", "").split("\n"):
            match = cls.query_server_status_cre.match(line.strip())
            if match:
                return match.group(1).strip()
        return None

    @classmethod
    def server_state(cls, out):
        """Just the state of the server status, without the `on command SUBMIT` half."""
        status = cls.server_status(out)
        return (status or "").split(" on command ")[0].strip().upper()

    @classmethod
    def server_warnings(cls, out):
        """The `Warning:` lines of a status response -- where a refusal states its reason.

        A refused task carries no `Failure message from server`: `tm_task_failure` stays empty and
        the TaskWorker uploads the reason as a task warning, which the client prints as `Warning:`.
        """
        return [
            line.split(":", 1)[1].strip()
            for line in (out or "").replace("\r", "").split("\n")
            if line.strip().startswith("Warning:")
        ]

    @classmethod
    def parse_query_output(cls, out, proj_dir, job_ids, skip_transfers=False):
        """Parse a status response, and say what it looked like when that fails.

        law's error names the server status it ended up with ("but got 'None'") but never the
        output it read, so an unreadable response cannot be diagnosed after the fact. Attach the
        head of it -- the status lines live in the first few lines, and the per-job JSON that
        follows is megabytes, so a slice is enough.

        A task the server has refused, and one it has merely not scheduled yet, are both reported
        by law as an unreadable status; they are told apart here, on the server status law itself
        extracted. The test happens only after law has refused the response, so a task that still
        publishes per-job JSON -- a large `FAILED` or `KILLED` one -- keeps its real job states.
        """
        try:
            return super(DSProdCrabJobManager, cls).parse_query_output(
                out, proj_dir, job_ids, skip_transfers=skip_transfers
            )
        except Exception as exc:
            state = cls.server_state(out)
            if state in cls.terminal_server_states:
                raise CrabTaskRefused(state, cls.server_warnings(out), proj_dir)
            if state in cls.pending_server_states:
                raise CrabTaskNotScheduledYet(state)
            head = [
                line[:200]
                for line in (out or "").replace("\r", "").split("\n")[:12]
                if not line.startswith("{")
            ]
            shown = "\n      ".join(head) or "<no output>"
            raise Exception(
                f"{exc}\n    first lines of what crab returned ({len(out or '')} bytes):"
                f"\n      {shown}"
            )

    #: per-site record to feed, injected by CrabWorkflow.crab_create_job_manager; None disables it
    site_stats = None

    #: cached CRIC site list to drop when a submission is refused, injected the same way
    site_cache_path = None

    #: in-flight counts of a project not queried for this long stop counting
    in_flight_stale_seconds = 3600.0

    def _apply_watchdog(self, result):
        """Turn a stalled job into a failed one, on this poll's fresh status.

        Rewriting the status here rather than editing law's job data is what makes the standard
        retry path do the work: law sees a failed job on this very iteration, counts the attempt,
        and hands the branches back to the wave gate like any other failure. Nothing changes the
        number of jobs law is polling, which its poll loop snapshots once.

        `code` is left as None deliberately. `harvest_site_stats` skips a failure with no job-level
        code -- "killed, or never started: not the site's doing" -- and a watchdog verdict is our
        own action, so it must not enter the site record through the back door. The site is
        recorded once per branch, explicitly, below.
        """
        watchdog = getattr(self, "watchdog", None)
        if watchdog is None or not watchdog.enabled:
            return
        for job_id, reason in watchdog.verdicts(result).items():
            data = result.get(job_id)
            if not isinstance(data, dict) or data.get("status") != self.RUNNING:
                # it finished in the seconds since the verdict was formed; resubmitting a branch
                # that is already done is the worst false positive available here
                continue
            site = ((data.get("extra") or {}).get("site_history") or [None])[-1]
            data["status"] = self.FAILED
            data["code"] = None
            data["error"] = reason
            watchdog.forget(job_id)
            msg = f"watchdog: failing job {job_id} -- {reason}"
            if site:
                msg += f" (last site {site})"
            print(msg)
            if site and getattr(self, "site_stats", None) is not None:
                # first stall of this branch only: a branch that hangs wherever it lands is the
                # branch's problem, and charging every one of its stalls to a different site is
                # how a quarantine baseline gets poisoned
                with self._stats_lock:
                    key = (str(job_id), "watchdog")
                    if key not in self._stats_seen:
                        self._stats_seen.add(key)
                        self.site_stats.record(site, False)
                        self.site_stats.save()

    def harvest_site_stats(self, proj_dir, result):
        """Record what CRAB itself said about each job, per site.

        Two things must not reach the record, and both are avoided by harvesting here rather
        than from `job_data` after the poll:

        * law's own bookkeeping. A resumed run flips every job whose outputs are gone to retry
          ("initially missing task outputs"), and a killed task reports its jobs as failed;
          neither says anything about a site. Harvested from `job_data` those became 8285
          failures in one poll, spread over every site of the production, and the resulting
          ~100 % baseline everywhere made the quarantine unable to fire for a genuinely broken
          site. A CRAB status response only ever carries what happened to the job, and a job
          that ended without a job-level error code (`Error` absent -- killed, or never ran)
          is skipped as well.
        * the wrong job. law syncs per-job `extra` onto `job_data` positionally
          (`law/workflow/remote.py`), so with more than one live CRAB project the
          `site_history` of one job can land on another. The parsed result is keyed by job id.

        Jobs still in flight are counted too -- not as outcomes, but as part of what was sent to
        a site, which is the denominator its failure rate is measured against.
        """
        if self.site_stats is None or not result:
            return
        in_flight = Counter()
        now = time.time()
        with self._stats_lock:
            for job_id, data in result.items():
                if not isinstance(data, dict):
                    continue
                history = (data.get("extra") or {}).get("site_history") or []
                if not history:
                    continue
                site = history[-1]
                status = data.get("status")
                if status == self.FINISHED:
                    ok = True
                elif status == self.FAILED and data.get("code") is not None:
                    ok = False
                elif status == self.FAILED:
                    # no job-level error code: killed, or never started -- not the site's doing
                    continue
                else:
                    in_flight[site] += 1
                    continue
                key = (str(job_id), ok)
                if key in self._stats_seen:
                    continue
                self._stats_seen.add(key)
                self.site_stats.record(site, ok)
            self._in_flight[proj_dir] = (now, in_flight)
            cutoff = now - self.in_flight_stale_seconds
            self._in_flight = {
                d: (ts, c) for d, (ts, c) in self._in_flight.items() if ts >= cutoff
            }
            combined = Counter()
            for _, counts in self._in_flight.values():
                combined.update(counts)
            self.site_stats.set_in_flight(combined)
            self.site_stats.save()

    def query(self, proj_dir, job_ids=None, *args, **kwargs):
        proj_dir = str(proj_dir)
        last_error = None
        for attempt in range(self.query_retries + 1):
            try:
                result = super(DSProdCrabJobManager, self).query(
                    proj_dir, job_ids=job_ids, *args, **kwargs
                )
            except CrabTaskRefused as exc:
                # terminal: retrying the query, and waiting between attempts, can only repeat it
                return self._refused(exc, proj_dir, job_ids)
            except CrabTaskNotScheduledYet as exc:
                # not an error at all, so neither the delay nor the unreadable count applies
                return self._not_scheduled_yet(exc, proj_dir, job_ids)
            except Exception as exc:
                # law raises before the response is parsed when the client exits non-zero, and the
                # output it read is inside the message: a refusal must be recognised there too, or
                # the retry storm this replaces comes back whenever crab reports a failing exit
                state = self.server_state(str(exc))
                if state in self.terminal_server_states:
                    return self._refused(
                        CrabTaskRefused(
                            state, self.server_warnings(str(exc)), proj_dir
                        ),
                        proj_dir,
                        job_ids,
                    )
                last_error = exc
                if attempt < self.query_retries:
                    time.sleep(self.query_retry_delay)
                continue
            self._unreadable.pop(proj_dir, None)
            self._unscheduled.pop(proj_dir, None)
            self._apply_watchdog(result)
            self.harvest_site_stats(proj_dir, result)
            return result

        n = self._unreadable.get(proj_dir, 0) + 1
        self._unreadable[proj_dir] = n
        if n > self.max_unreadable_polls:
            raise Exception(
                f"the status of {os.path.basename(proj_dir)} has been unreadable for {n} "
                f"consecutive polls; last error: {last_error}"
            )
        print(
            f"could not read the status of {os.path.basename(proj_dir)} "
            f"({n}/{self.max_unreadable_polls} consecutive), keeping its jobs pending: {last_error}"
        )
        return self._all_pending(proj_dir, job_ids)

    def _invalidate_site_cache(self):
        """Drop the cached site list, so the next submission asks CRIC again."""
        path = self.site_cache_path
        if not path:
            return
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            # the report says the list was dropped, so a failure to drop it must not be silent
            print(f"could not drop the cached site list {path}: {exc}")

    def _refusal_report(self, exc):
        """Everything an operator needs to act, in one message.

        The server names only the first site it objected to, so the list it was given matters as
        much as the objection: a second bad name would otherwise surface one submission later.
        """
        lines = [
            f"the CRAB server refused this submission ({exc.state}). It will never run, so its "
            "jobs are reported failed and law will submit them as a new task.",
            f"  project:  {exc.proj_dir}",
        ]
        for warning in exc.warnings or ["<the server gave no reason>"]:
            lines.append(f"  server:   {warning}")
        if self.site_cache_path:
            lines.append(
                f"  sites:    computed from {self.site_cache_path}, now dropped so the next "
                "submission re-reads CRIC"
            )
        lines.append(
            "  check:    the whitelist must contain only CMS Processing Site Names -- CRIC "
            "'?json&preset=site-names', rows with type 'psn'"
        )
        return "\n".join(lines)

    def _all_pending(self, proj_dir, job_ids):
        """Every job of the project reported pending -- what law does for a task with no jobs yet."""
        if job_ids is None:
            job_ids = self._job_ids_from_proj_dir(proj_dir)
        return {
            job_id: self.job_status_dict(job_id=job_id, status=self.PENDING)
            for job_id in job_ids
        }

    def _not_scheduled_yet(self, exc, proj_dir, job_ids):
        """A task the server has accepted but not handed to a scheduler: its jobs are pending.

        Not an error, so neither the retry delay nor `max_unreadable_polls` applies -- but not
        free either. The wait is repeated in the log while it lasts and bounded, because a task
        that never leaves this status would otherwise stall the production in silence, which is
        the failure this class exists to prevent.
        """
        self._unreadable.pop(proj_dir, None)
        n = self._unscheduled.get(proj_dir, 0) + 1
        self._unscheduled[proj_dir] = n
        if n > self.max_unscheduled_polls:
            self.stop_reason = (
                f"{os.path.basename(proj_dir)} has been {exc.state} for {n} consecutive polls "
                "without reaching a scheduler. The CRAB server accepted it, so this is not a "
                "configuration fault; the TaskWorker is the place to look."
            )
        elif n == 1 or n % self.unscheduled_report_every == 0:
            print(
                f"{os.path.basename(proj_dir)}: {exc} ({n} polls); its jobs stay pending"
            )
        return self._all_pending(proj_dir, job_ids)

    def _note_once(self, key, message):
        """Print `message` the first time `key` produces it: a status repeats every poll."""
        if key not in self._noted:
            self._noted.add(key)
            print(message)

    def _refused(self, exc, proj_dir, job_ids):
        """Report the jobs of a refused task as failed, so law resubmits them as a new task.

        `code` is left None on purpose, as for a watchdog verdict: `harvest_site_stats` charges a
        site only for a failure carrying a job-level code, and a task the server never scheduled
        ran nowhere. The error text is deliberately not law's "initially missing task outputs",
        which `StopOnMassInitialRetryProxy` counts.
        """
        self._unreadable.pop(proj_dir, None)
        self._refused_projects.add(proj_dir)
        # the whitelist is computed from a cached site list, and a name CRAB does not know is the
        # likeliest reason for a refusal, so the next submission must not reuse that cache
        self._invalidate_site_cache()
        self._note_once(("refused", proj_dir), self._refusal_report(exc))
        if len(self._refused_projects) >= self.max_refused_submissions:
            # recorded, not raised: `crab_poll_callback` is the one hook law lets an exception out
            # of. The jobs are still reported failed below, so the state law sees stays consistent
            # whichever way the run ends.
            self.stop_reason = (
                f"{len(self._refused_projects)} CRAB submissions have been refused by the server, "
                "so the next one would be too: this is a configuration fault, not bad luck.\n"
                + self._refusal_report(exc)
            )
        if job_ids is None:
            job_ids = self._job_ids_from_proj_dir(proj_dir)
        return {
            job_id: self.job_status_dict(
                job_id=job_id, status=self.FAILED, code=None, error=str(exc)
            )
            for job_id in job_ids
        }


class DSProdCrabJobFileFactory(law.cms.CrabJobFileFactory):
    """CRAB job file with no CRAB-side product/log transfer (DSProd owns remote I/O)."""

    #: files law copies out of its own tree into every submission
    law_sources = ("crab/crab_wrapper.sh", "crab/PSet.py")

    #: attempts, and the pause between them, before the software tree is given up on
    source_retries = 5
    source_retry_delay = 3.0

    @classmethod
    def missing_law_source(cls, retries=None, delay=None):
        """The first of law's own CRAB sources that cannot be read, or None if all can.

        law's own tree lives under `soft/` in the checkout that drives the production, so a
        submission can hit a moment when the storage holding it does not answer. The error that
        surfaces from law then is a copy of a path that never existed, so this names the real
        reason instead. No cause is diagnosed here: `os.path.isfile` answers `False` for a missing
        path, a refused one and a failing mount alike.
        """
        retries = cls.source_retries if retries is None else retries
        delay = cls.source_retry_delay if delay is None else delay
        base = os.path.dirname(os.path.abspath(law.contrib.cms.job.__file__))
        for rel in cls.law_sources:
            path = os.path.join(base, rel)
            for attempt in range(retries + 1):
                if os.path.isfile(path):
                    break
                if attempt < retries:
                    time.sleep(delay)
            else:
                return path
        return None

    @staticmethod
    def law_source_error(path):
        """What the storage answers for `path`, in the words a message can be acted on.

        The probe above cannot say why it failed, and the difference decides who has to fix it, so
        the errno is read once here rather than asked for in the message.
        """
        try:
            os.stat(path)
        except OSError as e:
            return f"[Errno {e.errno}] {e.strerror}"
        # the probe and this stat are seconds apart, so a path that blipped is answered for again
        return "stat succeeds now -- not a regular file, or the path came back"

    @classmethod
    def _wait_for_law_sources(cls):
        """Last resort: refuse to build a job file against a tree that is not there.

        `DSProdCrabWorkflowProxy.submit` checks the same sources before law is allowed to touch
        its job data, so reaching this raise means the tree went away inside one submission.
        """
        path = cls.missing_law_source()
        if path is not None:
            raise RuntimeError(
                f"{path} is not readable ({cls.law_source_error(path)}), so no CRAB job file "
                "can be built. law's own tree is unreachable: ENOENT points at the tree or its "
                "mount, EACCES/EPERM at the credential that storage is reached with."
            )

    def create(self, **kwargs):
        self._wait_for_law_sources()
        kwargs = dict(kwargs)
        kwargs["output_files"] = []
        job_file, c = super().create(**kwargs)
        if hasattr(c, "crab"):
            c.crab.General.transferOutputs = False
            c.crab.General.transferLogs = False
            if getattr(c.crab, "JobType", None) is not None:
                c.crab.JobType.sendPythonFolder = None
                c.crab.JobType.outputFiles = None
                c.crab.JobType.disableAutomaticOutputCollection = True
        c.output_files = []
        # the config-object tweaks above are not reflected in the already-written crab cfg, so
        # rewrite it: strip the deprecated sendPythonFolder (rejected by modern CRAB) and force
        # no CRAB-side transfers (DSProd owns remote I/O).
        try:
            self._rewrite_crab_job_file(job_file)
        except Exception as exc:
            print(f"WARNING: could not post-process crab job file {job_file}: {exc}")
        return job_file, c

    @staticmethod
    def _rewrite_crab_job_file(job_file):
        with open(job_file) as f:
            lines = f.readlines()
        new_lines = []
        skip_list = False
        for ln in lines:
            stripped = ln.strip()
            if "sendPythonFolder" in ln:
                continue
            if "General.transferOutputs" in ln:
                new_lines.append("cfg.General.transferOutputs = False\n")
                continue
            if "General.transferLogs" in ln:
                new_lines.append("cfg.General.transferLogs = False\n")
                continue
            if "JobType.outputFiles" in ln:
                if stripped.endswith("[") or ("[" in stripped and "]" not in stripped):
                    skip_list = True
                continue
            if skip_list:
                if "]" in stripped:
                    skip_list = False
                continue
            new_lines.append(ln)
        with open(job_file, "w") as f:
            f.writelines(new_lines)


#: CRIC's site table, asked the way CRAB asks it. `preset=site-names` with `type == "psn"` is
#: literally `WMCore.Services.CRIC.CRIC.getAllPSNs`, which the CRAB TaskWorker calls to validate a
#: whitelist (`TaskWorker/Actions/SiteInfoResolver.py`).
_CRIC_URL = "https://cms-cric.cern.ch/api/cms/site/query/?json&preset=site-names"

#: how long a cached site list is reused before CRIC is asked again
_CRIC_CACHE_SECONDS = 24 * 3600

#: a parse yielding fewer names than this is treated as a failed fetch. CRIC lists 119 processing
#: sites, so this cannot be reached by sites going offline -- only by the payload changing shape,
#: which would otherwise shrink the whitelist silently instead of raising.
_CRIC_MIN_SITES = 50


def _parse_cric_sites(payload):
    """Processing site names out of a `preset=site-names` payload.

    The preset answers `{"desc": {"columns": [...]}, "result": [[...], ...]}` -- rows are lists,
    not objects -- so the columns are read by name: their order is CRIC's to change.
    """
    columns = (payload or {}).get("desc", {}).get("columns") or []
    rows = (payload or {}).get("result") or []
    if not columns or not isinstance(rows, list):
        return []
    entries = [
        dict(zip(columns, row)) for row in rows if isinstance(row, (list, tuple))
    ]
    return sorted(
        {e["alias"] for e in entries if e.get("type") == "psn" and e.get("alias")}
    )


def _checked(sites, source):
    """`sites`, if it is long enough to be the real site list.

    Applied to every path out of `processing_sites`, cache included: a short list is not a small
    grid but a payload that changed shape, and it shrinks the whitelist without any error --
    `resolve_whitelist` only objects when a blacklist empties it completely.
    """
    n = len(sites) if isinstance(sites, list) else 0
    if n < _CRIC_MIN_SITES:
        raise RuntimeError(
            f"only {n} processing sites from {source}; expected at least "
            f"{_CRIC_MIN_SITES}, so the site pool would be silently shrunk"
        )
    return sites


def processing_sites(cache_path=None, url=_CRIC_URL, timeout=60):
    """CMS Processing Site Names, from CRIC, cached on disk.

    These are the names a `Site.whitelist` may contain. Any other name -- a storage endpoint such
    as `T1_US_FNAL_Disk`, or a compute resource CRAB does not treat as a processing site, such as
    `T3_CH_CERN_HelixNebula_REHA` -- makes the CRAB server refuse the whole task with "A site name
    ... is not in the list of known CMS Processing Site Names", which is terminal.

    An earlier version asked CRIC for every site carrying `computeunits`, reasoning that those are
    the ones that run jobs. They are not the same set: measured on 2026-09-13 it admitted three
    names that are not processing sites -- one of which refused a 36000-branch production -- while
    omitting 39 that are.
    """
    try:
        fresh = cache_path and (
            time.time() - os.path.getmtime(cache_path) < _CRIC_CACHE_SECONDS
        )
    except OSError:  # removed underneath, e.g. by a refusal dropping it
        fresh = False
    if fresh:
        try:
            with open(cache_path) as f:
                return _checked(json.load(f), cache_path)
        except (OSError, ValueError, RuntimeError):
            pass
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            sites = _checked(_parse_cric_sites(json.load(response)), url)
    except Exception as exc:
        if cache_path and os.path.exists(cache_path):
            age_h = (time.time() - os.path.getmtime(cache_path)) / 3600.0
            # announced, never silent: this is the path that resubmits yesterday's list, and a
            # list that is wrong is exactly what gets a submission refused
            print(
                f"could not read the CMS site list from {url} ({exc}); falling back to "
                f"{cache_path}, written {age_h:.1f} h ago"
            )
            with open(cache_path) as f:
                return _checked(json.load(f), cache_path)
        raise RuntimeError(f"could not read the CMS site list from {url}: {exc}")
    if cache_path:
        try:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(sites, f)
        except OSError:
            pass
    return sites


def resolve_whitelist(whitelist, blacklist, sites):
    """A `Site.whitelist` from which `blacklist` is actually absent.

    CRAB gives the whitelist precedence: a site matched by both is *kept*, and it says so only in a
    warning ("Since the whitelist has precedence, these sites are not considered in the blacklist").
    With the default all-tier globs that silently defeats every exclusion -- the configured
    `crab.blacklist` and the automatic site quarantine alike.

    So a tier glob covering an excluded site is expanded, from `sites`, into the sites it actually
    matches minus the excluded ones. Globs covering nothing excluded are left alone, which keeps the
    pool wide and the expansion small: excluding one T2 lists the T2s and leaves `T1_*` and `T3_*`
    as they are.
    """
    if not blacklist:
        return list(whitelist)
    out = []
    for entry in whitelist:
        hit = [b for b in blacklist if fnmatch.fnmatch(b, entry)]
        if not hit:
            out.append(entry)
            continue
        # an entry that is itself excluded simply disappears
        out += [
            site
            for site in sites
            if fnmatch.fnmatch(site, entry) and site not in blacklist
        ]
    if not out:
        raise RuntimeError(
            f"the blacklist {', '.join(blacklist)} excludes every site the whitelist "
            f"{', '.join(whitelist)} allows"
        )
    return out


_CrabProxyBase = law.cms.CrabWorkflow.workflow_proxy_cls


def _cli_has_parallel_jobs():
    """True when the user passed ``--parallel-jobs`` (or a task-prefixed form)."""
    parser = luigi.cmdline_parser.CmdlineParser.get_instance()
    tokens = list(getattr(parser, "cmdline_args", None) or [])
    for tok in tokens:
        if tok in ("--parallel-jobs", "--parallel_jobs"):
            return True
        if tok.startswith("--parallel-jobs=") or tok.startswith("--parallel_jobs="):
            return True
        if tok.endswith("-parallel-jobs") or tok.endswith("-parallel_jobs"):
            return True
        if "-parallel-jobs=" in tok or "-parallel_jobs=" in tok:
            return True
    return False


class DSProdCrabWorkflowProxy(
    ResyncExistingBranchesProxy, StopOnMassInitialRetryProxy, _CrabProxyBase
):
    def __init__(self, *args, **kwargs):
        super(DSProdCrabWorkflowProxy, self).__init__(*args, **kwargs)
        #: start of the release window of the retries the wave gate is holding back, or None
        #: while it holds none (see `_update_retry_release_clock`)
        self._retry_parked_since = None
        self._apply_crab_parallel_jobs()

    def _apply_crab_parallel_jobs(self):
        """Cap the jobs in flight, and therefore the size of one CRAB task.

        law's default is unlimited, and DSProd tasks also inherit `HTCondorWorkflow`, so the
        value can only be fixed here: a production with tens of thousands of branches would
        otherwise be submitted as a single CRAB task, far above what a task can hold.

        `crab.parallel_jobs: auto` derives it from the size of this workflow instead
        (`auto_parallel_jobs`); the branch map is needed by the submission anyway.
        """
        if _cli_has_parallel_jobs():
            return
        cfg_n = self.task._crab_cfg().get("parallel_jobs")
        if str(cfg_n).strip().lower() == _CRAB_AUTO_PARALLEL_JOBS:
            self._set_parallel_jobs(auto_parallel_jobs(len(self.task.get_branch_map())))
            return
        if cfg_n is not None:
            self._set_parallel_jobs(int(cfg_n))
            return
        if self.poll_data.n_parallel == self.n_parallel_max:
            self._set_parallel_jobs(_CRAB_DEFAULT_PARALLEL_JOBS)

    def _crab_refill_fraction(self):
        raw = self.task._crab_cfg().get(
            "refill_fraction", _CRAB_DEFAULT_REFILL_FRACTION
        )
        try:
            frac = float(raw)
        except (TypeError, ValueError):
            frac = _CRAB_DEFAULT_REFILL_FRACTION
        return min(max(frac, 0.0), 1.0)

    def _crab_retry_release_minutes(self):
        raw = self.task._crab_cfg().get(
            "retry_release_minutes", _CRAB_DEFAULT_RETRY_RELEASE_MINUTES
        )
        try:
            minutes = float(raw)
        except (TypeError, ValueError):
            minutes = _CRAB_DEFAULT_RETRY_RELEASE_MINUTES
        if not math.isfinite(minutes):
            # `waited >= nan` is never true, so a nan would disable the release silently
            minutes = _CRAB_DEFAULT_RETRY_RELEASE_MINUTES
        return max(minutes, 0.0)

    def _parked_retries(self):
        """The job numbers of retries the wave gate is holding back in `unsubmitted_jobs`.

        `job_data.attempts` is law's per-job retry counter and part of the submission file, and
        law increments it before a retry ever reaches `submit`, so it is what tells a parked
        retry from a never-submitted branch -- including across a restart, where the two arrive
        in the same `unsubmitted_jobs` mapping.
        """
        return set(self.job_data.unsubmitted_jobs) & set(self.job_data.attempts)

    def _update_retry_release_clock(self, after_release=False):
        """Keep a release window running exactly while the wave gate holds a retry back.

        The window has to be (re)started wherever the parked set changes, not only where this
        proxy parks a generation itself, because the two states that cost the most are invisible
        there: a release takes only as many parked retries as there are free slots and leaves the
        rest behind, and a resumed run reads them from the submission file while law hands it an
        empty retry generation on every poll (`law/workflow/remote.py`). A clock that only parking
        can start therefore never runs in either, which leaves the retries to the size gate --
        and with the driver dying about daily, a resumed leg is the normal case here.

        Only this timestamp lives in memory; the parked jobs themselves are in the submission
        file. A driver killed while retries are parked therefore finds them again and starts a
        fresh window for them -- a release delayed by at most one window, never a lost job.

        `after_release` starts that fresh window for the retries a release could not take, rather
        than leaving them with an already-expired one, which would open the gate on every poll
        and break the rest of the production into one CRAB task per polling interval.
        """
        if not self._parked_retries():
            self._retry_parked_since = None
        elif after_release or self._retry_parked_since is None:
            self._retry_parked_since = time.monotonic()

    def _parked_retries_are_due(self):
        """Whether the oldest retry parked by the wave gate has waited out its release window."""
        if self._retry_parked_since is None:
            return False
        waited = time.monotonic() - self._retry_parked_since
        return waited >= self._crab_retry_release_minutes() * 60

    def _should_submit_crab_group(self, n_backlog, n_retry):
        """Whether to submit now, or hold the jobs back so they accumulate into one CRAB task.

        Creating a CRAB task is expensive and a task holds only a few thousand jobs, so a
        production is submitted in waves of at least `refill_fraction * parallel_jobs` jobs. Jobs
        are held back only while such a wave is still **achievable**: once the work left in the
        whole production -- running plus waiting -- can no longer fill one, waiting can only delay
        it, so whatever is waiting goes out immediately, however little that is. That covers the
        tail of a large production and every small production (which can never fill a wave and so
        is never batched at all), while a trickle of retries early on still accumulates.

        Waiting work is counted in two parts. `n_backlog` is what sits in `unsubmitted_jobs` --
        never-submitted branches plus the retries a previous poll parked there -- and only it is
        measured against the wave size; `n_retry` is the generation of retries this poll offers,
        which has not waited for anything yet. Counting waiting work at all is what makes this an
        aggregation threshold: gating on free slots alone let a handful of retries out as their own
        CRAB task whenever the production did not fill `parallel_jobs` -- with 3270 of 5000 slots
        taken, 1730 were free, so the gate was open from the first poll onwards.

        A wave that is never reached must not park a retry for ever, though. Each generation a
        retry misses costs a full job length -- 7.1 h at the median here -- and over a 4800-job
        production the parked retries waited 11.35 h at the median for a wave that a handful of
        them could not fill. So a retry that has been parked for `crab.retry_release_minutes` goes
        out however small the wave it makes.
        """
        n_parallel = self.poll_data.n_parallel
        if n_parallel >= self.n_parallel_max:
            # unlimited parallelism: keep law's own behaviour
            return True
        n_waiting = n_backlog + n_retry
        if n_waiting <= 0:
            return True
        n_active = self.poll_data.n_active
        min_wave = self._crab_refill_fraction() * n_parallel
        # a full-sized wave, and the room to run it
        if min(n_backlog, n_parallel - n_active) >= min_wave:
            return True
        # even if every job still running were to fail, the next wave could not reach the bar
        if n_active + n_waiting < min_wave:
            return True
        return self._parked_retries_are_due()

    def submit(self, retry_jobs=None):
        # Before law is handed control: the job file is built inside law's submit(), and the
        # RuntimeError raised there for a tree that cannot be read is caught nowhere between it
        # and luigi, so one unreadable path fails the whole workflow and ends the driver -- which
        # it did on two consecutive days (2026-09-08, 2026-09-09). What law popped out of
        # `unsubmitted_jobs` just before costs nothing: between that pop and the job file the only
        # dump is the one on the nothing-to-submit early return, so the backlog on disk is intact
        # and a restart resumes. Skipping the round leaves every job where it was and the next
        # poll, minutes away, submits it. Probed once rather than waited out, because this runs
        # inside the poll loop. Skipping cannot hide a real outage for long: law dumps its job
        # data before it submits within one poll iteration, so where that dump shares the storage
        # law is installed on -- as it does in the production, both under `$ANALYSIS_PATH` -- one
        # skip is published and the next iteration's dump ends the run.
        missing = DSProdCrabJobFileFactory.missing_law_source(retries=1, delay=1.0)
        if missing is not None:
            reason = DSProdCrabJobFileFactory.law_source_error(missing)
            self.task.publish_message(
                f"law's own tree is unreachable ({missing}: {reason}); skipping this submission "
                "round -- nothing is lost, the next poll submits it. ENOENT points at the tree "
                "or its mount, EACCES/EPERM at the credential that storage is reached with -- "
                "`klist -f` shows both the expiry and the renewable window, which a running "
                "production only ever renews and never creates."
            )
            return OrderedDict()

        # explicitly, and before the wave gate: holding jobs back below returns without
        # delegating to the mixin, which would let a mass retry through on a later wave
        self.stop_on_mass_initial_retry(retry_jobs)
        retry_jobs = retry_jobs or OrderedDict()
        # before the gate is consulted, so that retries parked by an earlier poll or by a
        # previous driver are on the clock too, not only a generation parked right here
        self._update_retry_release_clock()
        if self._should_submit_crab_group(
            len(self.job_data.unsubmitted_jobs), len(retry_jobs)
        ):
            # law's submit() fills the wave up to `n_parallel` from `unsubmitted_jobs` whatever
            # opened the gate, so a release on the timer takes the never-submitted backlog with
            # it. That is wanted: the CRAB task is being created either way, and holding the
            # backlog back would only earn it a task of its own later. The backlog never starts a
            # clock of its own, so on its own it still waits for a full wave.
            submitted = super(DSProdCrabWorkflowProxy, self).submit(retry_jobs or None)
            # law took as many of the parked retries as it had free slots for; whatever is left
            # keeps waiting, and must keep waiting on a clock
            self._update_retry_release_clock(after_release=True)
            return submitted

        # park retries as unsubmitted, so the next eligible wave picks them up as one larger
        # CRAB task instead of creating a task for a handful of jobs now. `unsubmitted_jobs` is
        # where they have to wait: it is dumped to disk and `JobData.__len__` counts it, so a
        # killed driver finds them again and they stay part of the poll loop's `n_jobs` snapshot
        # -- a dict on this proxy would orphan them and shrink the production's total
        if retry_jobs:
            parked = OrderedDict()
            for job_num, branches in retry_jobs.items():
                if self._can_skip_job(job_num, branches):
                    continue
                self.job_data.jobs.pop(job_num, None)
                parked[job_num] = branches
            if parked:
                # in front of the backlog, because law's submit() fills up to `n_parallel` in dict
                # order: a retry appended behind tens of thousands of never-submitted branches
                # would not be reached for hours, and the release above could not get it out
                parked.update(self.job_data.unsubmitted_jobs)
                self.job_data.unsubmitted_jobs = parked
                self._update_retry_release_clock()
            self.dump_job_data()
        return OrderedDict()

    def setup_job_manager(self):
        """Gate submission on a valid VOMS proxy + a MyProxy credential the CRAB server needs."""
        proxy = os.environ.get("X509_USER_PROXY", "")
        if not proxy or not os.path.isfile(proxy):
            raise RuntimeError(
                "CRAB needs a VOMS proxy (X509_USER_PROXY). Run: "
                "voms-proxy-init --voms cms -valid 192:00"
            )
        if not law.wlcg.check_vomsproxy_validity(proxy_file=proxy):
            raise RuntimeError(
                f"VOMS proxy at {proxy} is expired. Run: "
                "voms-proxy-init --voms cms -valid 192:00"
            )
        kwargs = {"proxy": proxy}
        # CRABClient names the credential sha1(DN) and looks under no other name
        # (`CredentialInteractions.createNewMyProxy`), so one stored under the plain DN -- what a
        # bare `myproxy-init -d` leaves behind -- is invisible to the TaskWorker and must not
        # satisfy this gate.
        try:
            info = law.wlcg.get_myproxy_info(encode_username=True, silent=True) or {}
        except Exception:
            info = {}
        # law always submits with `crab submit --proxy <file>`, and that makes CRABClient skip its
        # own delegation and renewal outright (`SubCommand.handleMyProxy`), so nothing
        # in the submission path tops this credential up. 5 days is the TaskWorker's own minimum.
        if info.get("username") and info.get("timeleft", 0) >= 5 * 24 * 3600:
            kwargs["myproxy_username"] = info["username"]
            return kwargs
        raise RuntimeError(
            "CRAB requires a MyProxy credential valid for >= 5 days (the CRAB TaskWorker "
            "retrieves it from myproxy.cern.ch). Renew it with:\n"
            "  crab createmyproxy --days 30   # asks for the GRID certificate passphrase\n"
            "`myproxy-init` alone is not enough: it stores the credential under the plain DN and "
            "without the TaskWorker retrieval policy, so CRAB never sees it. To renew without "
            "the passphrase, delegate from the VOMS proxy instead -- the credential then expires "
            "with the proxy, so keep the proxy longer than 5 days:\n"
            "  X509_USER_CERT=$X509_USER_PROXY X509_USER_KEY=$X509_USER_PROXY \\\n"
            "      crab createmyproxy --days 7"
        )


class CrabWorkflow(law.cms.CrabWorkflow):
    """CRAB remote workflow mixin for DSProd tasks.

    A production of tens of thousands of branches is submitted as a series of CRAB tasks of
    `crab.parallel_jobs` jobs each (see `DSProdCrabWorkflowProxy`), so no manual chunking of the
    branch range is needed.
    """

    workflow_proxy_cls = DSProdCrabWorkflowProxy
    poll_interval = luigi.FloatParameter(default=5.0, significant=False)

    # A job CRAB reports as `transferring`/`transferred` has finished its payload and is only
    # waiting for a stageout that DSProd disables outright, so it must count as finished. law
    # otherwise decides that per poll by reading `config.JobType.disableAutomaticOutputCollection`
    # out of the project's `crab.log` (`CrabJobManager.query`): a log that is missing -- or one
    # regenerated by a later `crab` command without that line -- reads False, and those jobs are
    # then polled as running until the workflow gives up on them. Nothing about this production
    # makes it conditional, so it is pinned rather than parsed.
    crab_job_kwargs_query = {"skip_transfers": True}

    #: lazily-built, throttled `kinit -R` used while polling (see crab_poll_callback)
    _crab_kerberos_update = None

    #: the job manager of this run, set by `crab_create_job_manager`
    _dsprod_job_manager = None

    #: code tarball shipped to the workers, built once per law process (see _code_tarball)
    _code_tarball_path = None

    #: rolling per-site job statistics, fed by the job manager (see harvest_site_stats)
    _site_stats_obj = None

    #: stall watchdog, shared between the poll callback and the job manager (see job_watchdog)
    _watchdog_obj = None

    #: throttle for the watchdog's one directory listing per interval
    _watchdog_refresh = None

    crab_memory = luigi.IntParameter(
        default=-1,
        significant=False,
        description="memory per CRAB job in MB for this run; <= 0 = use the task's own `memory`, "
        "else `crab.memory_mb`, else CRAB's max(3000, 2500 * numCores)",
    )
    crab_whitelist = law.CSVParameter(
        default=(),
        significant=False,
        description="CRAB Site.whitelist; empty (default) = all CMS processing sites",
    )
    crab_blacklist = law.CSVParameter(default=(), significant=False)

    exclude_params_branch = getattr(
        law.cms.CrabWorkflow, "exclude_params_branch", set()
    ) | {
        "crab_memory",
        "crab_whitelist",
        "crab_blacklist",
    }

    def _crab_cfg(self):
        """CRAB site/resource settings from the merged global config (`config/global.yaml` +
        `user_custom.yaml`), NOT the production setup — so a setup is backend-agnostic and
        identical for htcondor and crab."""
        from .config import get_global

        cfg = get_global().get("crab", {}) or {}
        for legacy in ("max_memory_mb", "max_memory_mb_per_core"):
            if legacy in cfg:
                raise RuntimeError(
                    f"`crab.{legacy}` is no longer used and would now mean something different: "
                    "it used to seed a formula that immediately overwrote it, so its value was "
                    "never the request. Memory is now asked for per task -- set `memory` on the "
                    "task (or `--<task>-crab-memory` for one run, or `crab.memory_mb` as a global "
                    f"default) and delete `crab.{legacy}` from the config."
                )
        return cfg

    def _ensure_crab_pset(self, n_threads):
        """Minimal PSet whose numberOfThreads matches JobType.numCores (CRAB requires it)."""
        n_threads = max(1, int(n_threads))
        out_dir = self.local_path()
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"crab_PSet_threads{n_threads}.py")
        content = (
            "import FWCore.ParameterSet.Config as cms\n"
            'process = cms.Process("LAW")\n'
            'process.source = cms.Source("PoolSource", fileNames=cms.untracked.vstring([""]))\n'
            "process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(1))\n"
            "process.options = cms.untracked.PSet(\n"
            f"    numberOfThreads=cms.untracked.uint32({n_threads}),\n"
            "    numberOfStreams=cms.untracked.uint32(0),\n"
            ")\n"
        )
        if (not os.path.exists(path)) or open(path).read() != content:
            with open(path, "w") as f:
                f.write(content)
        return path

    def _code_tarball(self):
        """The code tarball shipped via CRAB inputFiles, built once per law process.

        A large production is submitted in several waves; rebuilding per wave would ship
        different code to different jobs if the checkout is touched meanwhile.
        """
        if self._code_tarball_path is None:
            out = os.path.join(self.local_path(), "dsprod_code.tar.gz")
            os.makedirs(os.path.dirname(out), exist_ok=True)
            self._code_tarball_path = build_code_tarball(self.ana_path(), out)
        return self._code_tarball_path

    def crab_stageout_location(self):
        """CRAB demands a `Site.storageSite` + `Data.outLFNDirBase` even when it transfers nothing.
        DSProd disables CRAB stageout and writes every product to `fs_default` (the same location
        as any other backend), so these are a submit-time formality and are filled in here rather
        than configured — there is deliberately no separate CRAB output location.
        """
        user = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
        return _CRAB_DUMMY_SITE, f"/store/user/{user}/DSProd_crab_unused"

    def crab_output_directory(self):
        return law.LocalDirectoryTarget(self.local_path())

    def crab_request_name(self, submit_jobs):
        # a large production is submitted as many CRAB tasks; naming them after the setup keeps
        # them identifiable in `crab status` and the monitoring dashboard
        name = "_".join(
            [
                self.task_family.replace(".", "_"),
                str(self.setup_name).replace(".", "_"),
                uuid.uuid4().hex[:8],
            ]
        )
        return re.sub(r"[^A-Za-z0-9_\-]", "_", name)[:100]

    def crab_bootstrap_file(self):
        from law.job.base import JobInputFile

        return JobInputFile(
            path=os.path.join(self.ana_path(), "bootstrap.sh"),
            copy=True,
            share=True,
            render_job=True,
        )

    def crab_workflow_requires(self):
        return {}

    def crab_check_job_completeness(self):
        return False

    def crab_poll_callback(self, poll_data):
        # The one hook the poll loop calls outside its own error handling, so the one place a
        # condition found while querying can actually end the run: law queries through a thread
        # pool and turns an exception there into the result, which would be counted as an
        # unreadable poll and nothing more. It runs after law's `finished` break, which only
        # matters if `acceptance` is ever lowered from 1.0: with it at 1.0 a poll that refuses a
        # task counts those jobs failed, so that poll can never be the finishing one.
        manager = self._dsprod_job_manager
        if manager is not None and manager.stop_reason:
            raise RuntimeError(manager.stop_reason)
        # a large CRAB production polls for days, while law keeps writing its job status files to
        # a work area whose storage authenticates every access — renew the Kerberos ticket as the
        # HTCondor backend does
        if self._crab_kerberos_update is None:

            def renew_kerberos_ticket():
                # verbose: a silent renewal leaves no way to tell, after a credential failure,
                # whether it had been running at all
                update_kerberos_ticket(verbose=1)

            krenew = float(getattr(self, "krenew", 1) or 0)
            self._crab_kerberos_update = (
                timed_call_wrapper(renew_kerberos_ticket, krenew * 3600)
                if krenew > 0
                else (lambda: None)
            )
        self._crab_kerberos_update()

        # one directory listing per interval, however many jobs are in flight. The verdicts
        # themselves are applied in the job manager's query(), on the fresh status of each CRAB
        # project, so nothing here changes the number of jobs law is polling.
        watchdog = self.job_watchdog()
        if watchdog.enabled and self._watchdog_refresh is None:
            self._watchdog_refresh = timed_call_wrapper(
                watchdog.refresh, watchdog.interval_seconds
            )
        if self._watchdog_refresh is not None:
            proxy = getattr(self, "workflow_proxy", None)
            job_data = getattr(proxy, "job_data", None)
            if job_data is not None:
                watchdog.set_jobs(getattr(job_data, "jobs", None))
            self._watchdog_refresh()
        return True

    def job_watchdog(self):
        """The stall watchdog, built once per law process and shared with the job manager.

        On for CRAB only, which is where the failure lives: a batch system reporting a slot as
        held after its payload has stopped. `crab.watchdog: false` turns it off; see
        `dsprod/watchdog.py` for what the settings mean.
        """
        if self._watchdog_obj is None:
            self._watchdog_obj = StallWatchdog(
                self.heartbeat_dir_uri,
                watchdog_config(self._crab_cfg()),
                voms_token=os.environ.get("X509_USER_PROXY") or None,
                publish=self.publish_message,
            )
        return self._watchdog_obj

    def crab_heartbeat(self):
        """Job side: refresh this branch's flag while the payload runs.

        A no-op unless this really is a CRAB job -- `LAW_CRAB_JOB_NUMBER` is set by law's CRAB
        wrapper and by nothing else -- so a local or HTCondor run writes no flags, matching the
        driver side, which only watches CRAB.
        """
        cfg = watchdog_config(self._crab_cfg())
        if (
            not cfg["enabled"]
            or not self.is_branch()
            or "LAW_CRAB_JOB_NUMBER" not in os.environ
        ):
            return contextlib.nullcontext()
        return Heartbeat(
            self.heartbeat_target(self.branch).uri(),
            int(cfg["interval_minutes"]) * 60,
            voms_token=os.environ.get("X509_USER_PROXY") or None,
            label={"task": self.task_family, "branch": self.branch},
            log=self.publish_message,
        )

    def site_stats(self):
        """Rolling per-site job record, kept in the production area across runs."""
        if self._site_stats_obj is None:
            self._site_stats_obj = SiteStats(
                os.path.join(self.ana_data_path(), "crab_site_stats.json"),
                self._crab_cfg().get("auto_blacklist"),
            )
        return self._site_stats_obj

    def site_cache_path(self):
        """Where the CRIC site list is cached.

        Deliberately not the old `cms_sites.json`: that file holds a list built by a different rule
        (see `processing_sites`), and reusing it would keep a refused submission refused for the
        whole cache lifetime after the rule was corrected.
        """
        return os.path.join(self.ana_data_path(), "cms_psn_sites.json")

    def crab_job_manager_cls(self):
        return DSProdCrabJobManager

    def crab_job_file_factory_cls(self):
        return DSProdCrabJobFileFactory

    def crab_create_job_manager(self, **kwargs):
        """Create the job manager, and build its CMSSW sandbox, before anything is submitted.

        law builds that sandbox lazily, inside every submission attempt. A failure there is
        swallowed per job: each one is stored with `dummy_job_id`, polled as "unknown job id",
        retried, and the workflow only dies when the retry tolerance is exceeded -- half an hour
        later, with the real cause nowhere in the log. Building it here turns that into a single
        actionable error before the first submission.
        """
        manager = super().crab_create_job_manager(**kwargs)
        manager.site_stats = self.site_stats()
        manager.watchdog = self.job_watchdog()
        manager.site_cache_path = self.site_cache_path()
        # kept so `crab_poll_callback` can see what the manager found while querying
        self._dsprod_job_manager = manager
        try:
            manager.cmssw_env
        except Exception as exc:
            raise RuntimeError(
                "could not set up the CMSSW sandbox that law runs `crab` in: "
                f"{exc}\nThis usually means `python` on PATH is not the DSProd shim (the "
                "sandbox dumps its environment with bare `python`, which CMSSW no longer "
                "ships). Source env.sh in this shell -- it writes soft/bin/python and prepends "
                "soft/bin to PATH -- and submit again."
            ) from exc
        return manager

    def crab_job_config(self, config, job_nums, branches=None):
        cfg = self._crab_cfg()
        mb_per_core = int(cfg.get("mb_per_core", CRAB_MB_PER_CORE))
        mb_single_core = int(cfg.get("mb_single_core", CRAB_MB_SINGLE_CORE))
        max_cores = _crab_cores_down(max(1, int(cfg.get("max_cores", 8))))
        n_cpus = max(1, int(getattr(self, "n_cpus", 1) or 1))

        # Memory is chosen independently of the cores: the first explicit value wins and nothing
        # raises it afterwards. Any value <= 0 (and a yaml `null`) means "not set".
        mem = 0
        for candidate in (
            int(self.crab_memory or 0),  # --<task>-crab-memory, this run only
            int(getattr(self, "memory", 0) or 0),  # the task's own request
            int(cfg.get("memory_mb", 0) or 0),  # a global default, if any
        ):
            if candidate > 0:
                mem = candidate
                break
        if 0 < mem < 1000:
            raise ValueError(
                f"{self.task_family}: a memory request of {mem} is read as MB, and {mem} MB per "
                "job cannot be meant -- pass the value in MB"
            )
        mem_auto = mem <= 0

        # Cores are the threads the payload runs, raised to whatever the memory request needs
        # (CRAB sells memory only in per-core units), snapped to a value CRAB accepts, then capped.
        needed = n_cpus if mem_auto else max(n_cpus, -(-mem // mb_per_core))
        n_cores = max(1, min(_crab_cores_up(needed), max_cores))

        # Only with no explicit request does the per-core formula apply, and only once the core
        # count is known.
        ceiling = max(mb_single_core, n_cores * mb_per_core)
        if mem_auto:
            mem = ceiling
        elif mem > ceiling:
            # Never clamp an explicit request down: CRAB enforces maxMemoryMB as a kill threshold
            # (PeriodicRemove on MemoryUsage > RequestMemory, exit 50660, never retried) and does
            # not escalate on retry, so a silently shrunk request is a silently dead branch. Fail
            # at submit instead, where it is one message rather than 4 lost attempts per job.
            want = _crab_cores_up(-(-mem // mb_per_core))
            raise ValueError(
                f"{self.task_family}: {mem} MB per job needs numCores >= {want}, but "
                f"crab.max_cores caps this task at {n_cores}, where CRAB allows at most "
                f"{ceiling} MB (max({mb_single_core}, numCores * {mb_per_core})). Raise "
                f"crab.max_cores to {want}, or ask for {ceiling} MB or less."
            )

        # the pset must declare exactly `numCores` threads or the client refuses the task
        config.crab.JobType.psetName = self._ensure_crab_pset(n_cores)
        config.crab.JobType.numCores = n_cores
        config.crab.JobType.maxMemoryMB = mem

        # ship the DSProd code (no AFS on WLCG workers). This MUST go through law's
        # input_files dict: the job-file factory rebuilds JobType.inputFiles from it and
        # would overwrite any value written directly to config.crab.JobType.inputFiles.
        # law_job.sh symlinks every input file into LAW_JOB_HOME (the bootstrap's CWD), so
        # the tarball lands exactly where bootstrap.sh looks for it. postfix=False keeps the
        # name `dsprod_code.tar.gz` the bootstrap checks; render=False (binary tarball).
        config.input_files["dsprod_code"] = JobInputFile(
            self._code_tarball(),
            copy=True,
            share=True,
            postfix=False,
            render=False,
        )

        max_runtime = getattr(self, "max_runtime", None)
        if max_runtime is not None and float(max_runtime) > 0:
            floor = int(self._crab_cfg().get("min_runtime_min", 60))
            config.crab.JobType.maxJobRuntimeMin = max(
                int(math.floor(float(max_runtime) * 60)), floor
            )

        whitelist = list(self.crab_whitelist) or list(
            self._crab_cfg().get("whitelist") or []
        )
        blacklist = list(self.crab_blacklist) or list(
            self._crab_cfg().get("blacklist") or []
        )
        # DSProd generation jobs have no real input dataset, so they can run at ANY CMS processing
        # site — but `ignoreLocality` below makes a whitelist mandatory for the CRAB client, so an
        # unset one becomes every tier rather than nothing. Configuring one can only narrow the
        # pool. Do NOT auto-whitelist the *storage* site either: it may not be a processing site
        # (e.g. T3_CH_CERNBOX) and CRAB then refuses the task ("not in the list of known CMS
        # Processing Site Names").
        # sites quarantined by their recent failure record; every wave is a new CRAB task, so
        # this takes effect for the next one -- retries included
        quarantined = [s for s in self.site_stats().blacklist() if s not in blacklist]
        if quarantined:
            self.publish_message(
                "keeping {} site(s) out of this CRAB task after recent failures: {}".format(
                    len(quarantined), ", ".join(quarantined)
                )
            )
            blacklist = list(blacklist) + quarantined

        sites = resolve_whitelist(
            whitelist or _CRAB_ALL_SITES,
            blacklist,
            processing_sites(self.site_cache_path()),
        )
        config.crab.Site.whitelist = [str(s) for s in sites]
        if blacklist:
            config.crab.Site.blacklist = [str(s) for s in blacklist]
        # Keep CMS's global blacklist of known-broken sites in force unless explicitly waived:
        # with an open site pool it is the main protection against burning jobs at bad sites.
        if self._crab_cfg().get("ignore_global_blacklist", False):
            config.crab.Site.ignoreGlobalBlacklist = True
        config.crab.Data.ignoreLocality = True
        return config
