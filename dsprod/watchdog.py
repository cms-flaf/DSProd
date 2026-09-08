"""Finding a job that still holds its slot after its payload has stopped.

CRAB reports a job as ``running`` for as long as the batch system says the slot is held, which is
not the same as the payload still doing anything. A production of 600 branches ended with two jobs
whose entire status record -- wall duration, memory, CPU time -- was byte-identical across eight
consecutive polls: they had started promptly, run for ~2.7 h and then stopped reporting, and
nothing would have reclaimed them until CRAB's 24 h wall-clock rule fired 21 hours later. The
production had 598 of 600 branches done and sat there.

The signal here is a flag file per running job in ONE flat directory on the same storage the job
must write its products to, refreshed by the job itself. Its modification time is the evidence, so
the driver needs exactly one directory listing per interval however many jobs are in flight (about
1.6 s at 3000 entries) and never reads a flag's content. A job whose flag has not moved for
``missed_checks`` intervals is declared failed, which puts it straight through law's ordinary
retry path -- there is no separate resubmission mechanism, and none is wanted.

Two things this deliberately does not do. It does not kill the job: ``crab kill`` has no per-job
form, and law's ``cancel`` ignores the ids it is given and kills the whole task, so condemning one
job would take every healthy sibling with it. The slot is abandoned instead, and CRAB's wall-clock
rule reclaims it. And it never condemns on the flag alone in bulk: if writing to the storage breaks
while reading it still works, every flag goes stale at once while the jobs are fine, so a listing
in which most running jobs look stale is read as an infrastructure fault and produces no verdicts.
"""

import datetime
import functools
import json
import os
import socket
import tempfile
import threading

from .grid_tools import gfal_copy, gfal_ls_safe, gfal_rm

#: the flat directory, under the production's own storage prefix, that holds one flag per live job
HEARTBEAT_DIR = "heartbeat"

DEFAULTS = {
    # on for CRAB and nothing else: the failure mode is a batch system holding a slot it cannot
    # account for, and a local run has no slot to hold
    "enabled": True,
    #: how often a job refreshes its flag, and how often the driver lists the directory
    "interval_minutes": 30,
    #: consecutive refreshes a job may miss before it is declared failed
    "missed_checks": 2,
    #: never condemn more than this many jobs in one interval, whatever the evidence says
    "max_per_interval": 5,
    #: how often one branch may be rescued this way before it is left to the wall-clock rule; a
    #: branch that stalls wherever it runs is the branch's problem, not the site's, and each
    #: verdict spends one of its four attempts
    "max_per_branch": 1,
    #: a listing in which this fraction of running jobs looks stale is an infrastructure fault
    "max_stale_fraction": 0.5,
    #: log the verdicts that would have been issued, issue none
    "dry_run": False,
}


def watchdog_config(backend_cfg):
    """The `watchdog` block of a backend config, merged over the defaults."""
    raw = (backend_cfg or {}).get("watchdog", {})
    if raw is False or raw is None:
        return dict(DEFAULTS, enabled=False)
    if raw is True:
        return dict(DEFAULTS)
    unknown = set(raw) - set(DEFAULTS)
    if unknown:
        raise RuntimeError(
            f"unknown watchdog setting(s) {sorted(unknown)}; known: {sorted(DEFAULTS)}"
        )
    cfg = dict(DEFAULTS, **raw)
    if int(cfg["interval_minutes"]) < 1 or int(cfg["missed_checks"]) < 1:
        raise RuntimeError(
            "watchdog.interval_minutes and watchdog.missed_checks must both be >= 1"
        )
    return cfg


class Heartbeat:
    """Job side: refresh one flag every `interval_seconds` for as long as the payload runs.

    Used as a context manager so the flag is dropped on the way out however the payload ends. A
    failure to write is never allowed to disturb the job: the whole point is to observe it, and a
    storage hiccup that killed the payload would be far worse than a missed beat.
    """

    def __init__(self, uri, interval_seconds, voms_token=None, label=None, log=None):
        self.uri = uri
        self.interval = max(1.0, float(interval_seconds))
        self.voms_token = voms_token
        self.label = label or {}
        self.log = log or (lambda msg: None)
        self._stop = threading.Event()
        self._thread = None
        self._beats = 0

    def _write(self):
        # the driver only ever reads the modification time; the content is for a human looking at
        # a verdict after the fact
        payload = dict(
            self.label,
            beat=self._beats,
            utc=datetime.datetime.utcnow().replace(microsecond=0).isoformat(),
            host=socket.gethostname(),
            pid=os.getpid(),
        )
        fd, path = tempfile.mkstemp(prefix="dsprod-beat-")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f)
            # force: gfal-copy does not overwrite, and an in-place overwrite is the signal -- a
            # remove-then-copy would leave a window in which the flag is simply absent, which the
            # driver cannot tell from a stalled job
            gfal_copy(path, self.uri, voms_token=self.voms_token, force=True, verbose=0)
            self._beats += 1
        finally:
            os.unlink(path)

    def _loop(self):
        while True:
            try:
                self._write()
            except Exception as exc:  # never let the heartbeat break the payload
                self.log(f"heartbeat: could not refresh {self.uri}: {exc}")
            if self._stop.wait(self.interval):
                return

    def __enter__(self):
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
        try:
            gfal_rm(self.uri, voms_token=self.voms_token, verbose=0)
        except Exception:
            # a flag left behind is harmless: the driver only condemns jobs it is still polling,
            # and the sweep collects flags whose job is gone
            pass
        return False


def with_heartbeat(run):
    """Refresh this branch's flag for as long as `run` is executing.

    A decorator rather than a `with` inside each body: it covers every path out of the payload --
    return, raise, and the batch-node guards that leave early -- without touching the bodies, and
    it makes the fact that a task is watched visible on the method itself.
    """

    @functools.wraps(run)
    def wrapper(self, *args, **kwargs):
        with self.crab_heartbeat():
            return run(self, *args, **kwargs)

    return wrapper


class StallWatchdog:
    """Driver side: one listing per interval, and a verdict for jobs whose flag has stopped moving.

    `refresh()` runs from the poll callback (throttled there) and `verdicts()` from the job
    manager's `query()`, which law calls once per CRAB project **concurrently**, so every piece of
    state here is taken under one lock.
    """

    def __init__(self, flag_dir_uri, cfg=None, voms_token=None, publish=None):
        self.flag_dir = flag_dir_uri
        self.cfg = dict(DEFAULTS) if cfg is None else dict(cfg)
        self.voms_token = voms_token
        self.publish = publish or (lambda msg: None)
        self._lock = threading.Lock()
        self._ages = None  # branch name -> mtime, from the last listing that worked
        self._listed = False
        self._first_running = {}  # (job_num, job_id) -> when we first saw it running
        self._per_branch = {}  # branch -> verdicts issued so far
        self._by_id = {}  # (crab_num, task_name) -> (job_num, branches)
        self._issued_this_interval = 0

    @property
    def enabled(self):
        return bool(self.cfg.get("enabled"))

    @property
    def interval_seconds(self):
        return int(self.cfg["interval_minutes"]) * 60

    @property
    def stale_seconds(self):
        return self.interval_seconds * int(self.cfg["missed_checks"])

    def refresh(self):
        """List the flag directory once. Returns False if the listing could not be read."""
        if not self.enabled:
            return False
        entries = gfal_ls_safe(self.flag_dir, voms_token=self.voms_token, verbose=0)
        with self._lock:
            self._issued_this_interval = 0
            if entries is None:
                # Could be an outage, could be a directory no job has written to yet. Either way
                # there is no evidence, and stale evidence must not accumulate across it.
                self._ages = None
                self._listed = False
                return False
            self._ages = {
                e.name: e.date for e in entries if e.date is not None and not e.is_dir
            }
            self._listed = True
            return True

    def _age(self, branches, now):
        """Seconds since the freshest flag of `branches`, or None if none of them has one."""
        stamps = [self._ages.get(str(b)) for b in branches]
        stamps = [s for s in stamps if s is not None]
        if not stamps:
            return None
        # a timestamp in the future (skew between driver and storage) counts as fresh
        return max(0.0, (now - max(stamps)).total_seconds())

    @staticmethod
    def _key(job_id):
        """A key that matches law's `JobId` namedtuple and its json form in job_data.

        Both are (crab_num, task_name, proj_dir) in that order, but one arrives as a namedtuple
        from the job manager and the other as a list from the dumped job data, and the project
        directory of the same job can differ between them (law rewrites it on resubmission).
        """
        parts = tuple(job_id)[:2]
        return (int(parts[0]), str(parts[1]))

    def set_jobs(self, jobs):
        """Publish law's job_data mapping so verdicts can name the branches behind a job id."""
        by_id = {}
        for job_num, entry in (jobs or {}).items():
            job_id = (entry or {}).get("job_id")
            branches = (entry or {}).get("branches") or []
            if not job_id or not branches:
                continue
            try:
                by_id[self._key(job_id)] = (job_num, list(branches))
            except (TypeError, ValueError, IndexError):
                continue
        with self._lock:
            self._by_id = by_id

    def verdicts(self, result, now=None):
        """Which of the jobs `result` reports running have stopped refreshing their flag.

        `result` is the status dict the job manager just fetched, keyed by law's `JobId`. Returns
        {that same key: reason} for the jobs to be failed.
        """
        if not self.enabled:
            return {}
        now = now or datetime.datetime.utcnow()
        with self._lock:
            if not self._listed or self._ages is None:
                return {}
            running, stale = [], []
            for job_id, data in result.items():
                if not isinstance(data, dict) or data.get("status") != "running":
                    continue
                try:
                    known = self._by_id.get(self._key(job_id))
                except (TypeError, ValueError, IndexError):
                    continue
                if not known:
                    continue
                job_num, branches = known
                key = self._key(job_id)
                self._first_running.setdefault(key, now)
                running.append(job_id)
                age = self._age(branches, now)
                # a job that has not had time to write its first flag is not evidence of anything
                since_seen = (now - self._first_running[key]).total_seconds()
                grace = self.stale_seconds + self.interval_seconds
                if age is None:
                    if since_seen >= grace:
                        stale.append((job_id, job_num, branches, None))
                elif age >= self.stale_seconds:
                    stale.append((job_id, job_num, branches, age))
            if not stale:
                return {}
            # writing to the storage can break while reading it still works, and then every flag
            # goes stale at once while every job is healthy
            fraction = float(self.cfg["max_stale_fraction"])
            if running and len(stale) > max(1, int(fraction * len(running))):
                self.publish(
                    f"watchdog: {len(stale)} of {len(running)} running jobs have a stale "
                    f"heartbeat -- reading that as a storage fault, not {len(stale)} dead jobs, "
                    "and issuing no verdicts"
                )
                return {}
            out = {}
            for job_id, job_num, branches, age in sorted(
                stale, key=lambda s: str(s[1])
            ):
                if self._issued_this_interval >= int(self.cfg["max_per_interval"]):
                    self.publish(
                        "watchdog: reached max_per_interval "
                        f"({self.cfg['max_per_interval']}); leaving the rest for the next interval"
                    )
                    break
                repeat = max(self._per_branch.get(b, 0) for b in branches)
                if repeat >= int(self.cfg["max_per_branch"]):
                    self.publish(
                        f"watchdog: branch(es) {list(branches)} have stalled {repeat + 1} times "
                        "now -- that is the branch, not the slot, so it is left to CRAB's "
                        "wall-clock limit instead of spending another attempt"
                    )
                    continue
                seen = (
                    "no heartbeat"
                    if age is None
                    else f"heartbeat {age / 60:.0f} min old"
                )
                reason = (
                    f"stalled: {seen}, threshold {self.stale_seconds / 60:.0f} min "
                    f"({self.cfg['missed_checks']} x {self.cfg['interval_minutes']} min)"
                )
                if self.cfg.get("dry_run"):
                    self.publish(
                        f"watchdog (dry run): would fail job {job_num} -- {reason}"
                    )
                    continue
                for b in branches:
                    self._per_branch[b] = self._per_branch.get(b, 0) + 1
                self._issued_this_interval += 1
                out[job_id] = reason
            return out

    def forget(self, job_id):
        """Drop the grace clock of a job id law has replaced, so a resubmission starts clean."""
        with self._lock:
            self._first_running.pop(self._key(job_id), None)
