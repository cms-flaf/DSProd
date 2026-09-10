"""Rolling per-site job statistics and dynamic CRAB site blacklisting.

A single broken worker node fails jobs in seconds, frees its slot and picks up the next one, so one
bad host can eat a large share of a production: on 2026-08-27 `comp-g-001.local` at T2_EE_Estonia
failed 258 jobs with `/usr/bin/base64: Input/output error`, before any physics ran. CRAB accepts a
blacklist only per *site* and only at submission time, so DSProd keeps its own record in the
production area and quarantines a site whose recent jobs mostly fail. Since every wave is a new
CRAB task, the next wave -- retries included -- is submitted without it.

A site's failure rate is measured against every job *sent* there -- the ones that already ended
plus the ones still in flight. Counting only finished jobs is what a first version did, and it does
not work: a job fails in seconds and succeeds in hours, so early in a production every site's
finished set is ~100 % failures, no site looks worse than the others, and nothing is ever
quarantined (observed with 335/391 failures at one site and 0.7-8 % everywhere else).

Configured under `crab.auto_blacklist` in the merged global config; see `DEFAULTS`.
"""

import json
import os
import re
import time

#: `crab.auto_blacklist` settings and their defaults
#: CMS site names, e.g. T1_DE_KIT, T2_UK_London_IC. CRAB reports "Unknown" when it does not know
#: where a job ran; feeding that back as a blacklist entry would be meaningless at best and could
#: have the client reject the whole submission.
_SITE_RE = re.compile(r"^T\d_[A-Za-z0-9_]+$")


def is_site(name):
    """Whether `name` is a CMS site name rather than a placeholder such as "Unknown"."""
    return bool(name and _SITE_RE.match(str(name)))


DEFAULTS = {
    # set false to keep only the statically configured `crab.blacklist`
    "enabled": True,
    # a site needs at least this many failures before it can be quarantined at all
    "min_failures": 5,
    # ... and at least this fraction of the jobs sent there (ended + in flight) must have failed
    "min_failure_rate": 0.5,
    # ... and it must be failing this many times more often than the other sites, so a bug of our
    # own -- which fails everywhere -- cannot blacklist every site that runs it
    "relative_factor": 2.0,
    # ... judged against at least this many jobs elsewhere. Without a baseline the first site to
    # collect `min_failures` would be quarantined on its own record alone, before there is anything
    # to compare it with; with a single site there is also nowhere else to send the work.
    "min_baseline_jobs": 20,
    # how long a site's FIRST quarantine lasts. Each further one doubles the last, up to
    # `max_quarantine_hours`: the count is kept when a quarantine is lifted, so a site that is
    # still broken is held out for longer and longer instead of returning on a fixed timer. A
    # 6-hour fixed ban let three sites that failed 93-98 % of everything sent to them cycle back
    # into the whitelist three times each over 2026-09-08..10, eating a wave every time.
    "quarantine_hours": 24.0,
    # the ceiling the doubling stops at -- 32 days
    "max_quarantine_hours": 768.0,
    # outcomes older than this stop counting
    "window_hours": 24.0,
    # never quarantine more than this many sites at once
    "max_sites": 10,
}


def resolve_config(cfg):
    """Merge a user `crab.auto_blacklist` mapping onto `DEFAULTS`."""
    out = dict(DEFAULTS)
    if isinstance(cfg, bool):
        out["enabled"] = cfg
    elif cfg:
        out.update({k: v for k, v in cfg.items() if k in DEFAULTS})
    return out


class SiteStats:
    """Job outcomes per site, persisted as JSON, with a rolling window and quarantines."""

    def __init__(self, path, cfg=None):
        self.path = path
        self.cfg = resolve_config(cfg)
        self.sites = {}
        #: jobs currently pending or running per site; part of the denominator, never persisted
        self.in_flight = {}
        self._dirty = False
        self.load()

    # -- persistence ------------------------------------------------------------------------

    def load(self):
        """Read the persisted record, dropping every entry that does not read back.

        Nothing in here may raise. The file is advisory -- it rebuilds within a poll or two -- and
        `load` runs from `__init__`, i.e. from `crab_create_job_manager` while a CRAB workflow is
        being submitted, so a file this version cannot read (unparseable JSON, or an `events`
        entry another version shaped differently) would stop a production before its first job
        over data nothing depends on.
        """
        try:
            with open(self.path) as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        sites = data.get("sites") if isinstance(data, dict) else None
        if not isinstance(sites, dict):
            return
        loaded = {}
        for name, rec in sites.items():
            # a state file written before placeholders were filtered may hold "Unknown"
            if not is_site(name) or not isinstance(rec, dict):
                continue
            try:
                loaded[name] = {
                    "events": [
                        (float(t), int(ok)) for t, ok in (rec.get("events") or [])
                    ],
                    "quarantined_until": float(rec.get("quarantined_until") or 0.0),
                    # absent from a record written before quarantines escalated: such a site
                    # starts at the base duration, which is what it would have had anyway
                    "quarantines": int(rec.get("quarantines") or 0),
                    "cleared_at": float(rec.get("cleared_at") or 0.0),
                }
            except (TypeError, ValueError):
                continue
        self.sites = loaded

    def save(self):
        if not self._dirty:
            return
        tmp = f"{self.path}.tmp"
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(tmp, "w") as f:
            json.dump({"version": 1, "sites": self.sites}, f)
        os.replace(tmp, self.path)
        self._dirty = False

    # -- recording --------------------------------------------------------------------------

    def set_in_flight(self, counts):
        """Jobs still pending or running per site, as of the latest poll."""
        self.in_flight = {s: n for s, n in (counts or {}).items() if is_site(s)}

    def record(self, site, ok, now=None):
        """Note one finished (`ok=True`) or failed job at `site`."""
        if not is_site(site):
            return
        now = time.time() if now is None else now
        rec = self.sites.setdefault(site, self._new_record())
        rec["events"].append((float(now), int(bool(ok))))
        self._dirty = True
        self._prune(now)
        # judging happens in `blacklist()`, once the caller has also reported what is still in
        # flight -- doing it here would use a stale, usually empty, denominator

    # -- blacklisting -----------------------------------------------------------------------

    def blacklist(self, now=None):
        """The sites to keep out of the next submission, worst first."""
        if not self.cfg["enabled"]:
            return []
        now = time.time() if now is None else now
        self._prune(now)
        self._expire(now)
        # re-judge here as well: the in-flight counts move between polls even when nothing new fails
        self._quarantine(now)
        active = [
            (name, rec)
            for name, rec in self.sites.items()
            if rec["quarantined_until"] > now
        ]
        # most failures first, so the cap keeps the worst offenders
        active.sort(key=lambda item: -self._counts(item[0], item[1])[1])
        return [name for name, _ in active[: int(self.cfg["max_sites"])]]

    # -- internals --------------------------------------------------------------------------

    @staticmethod
    def _new_record():
        return {
            "events": [],
            "quarantined_until": 0.0,
            #: quarantines served, which is what the next one's length doubles on
            "quarantines": 0,
            #: when the last quarantine was lifted; outcomes before it no longer judge the site
            "cleared_at": 0.0,
        }

    def _counts(self, site, rec, since=0.0):
        """(jobs sent to `site`, failures among them): ended jobs plus the ones still in flight.

        `since` drops the outcomes recorded before a moment -- the end of the last quarantine.
        The record itself is kept there (a quarantine doubles on how many came before it), so
        without this a site would be re-quarantined the instant its ban lifted, on the very
        evidence that ban was served for, and would never get the second chance it is given.
        It filters the ended outcomes only: a job dispatched before a ban can still be running
        when it lifts, and there is no way to tell from a count. That biases the fresh rate
        downwards, i.e. towards leaving the site in, and it resolves itself as those jobs end.
        """
        events = [e for e in rec["events"] if e[0] >= since] if since else rec["events"]
        n_fail = sum(1 for _, ok in events if not ok)
        return len(events) + self.in_flight.get(site, 0), n_fail

    def _prune(self, now):
        cutoff = now - float(self.cfg["window_hours"]) * 3600.0
        for rec in self.sites.values():
            kept = [(t, ok) for t, ok in rec["events"] if t >= cutoff]
            if len(kept) != len(rec["events"]):
                rec["events"] = kept
                self._dirty = True

    def _expire(self, now):
        """Lift quarantines that have run out, without forgetting that they happened.

        Wiping the record here is what made a fixed ban useless against a site that stays broken:
        it returned with a clean sheet, had to earn `min_failures` all over again, and bought
        itself another wave each time. The count is therefore kept and drives the next ban's
        length; only the *evidence* stops judging the site, through `cleared_at`, so that the site
        is measured on the outcomes recorded after the ban rather than the ones that earned it.
        """
        for rec in self.sites.values():
            if 0.0 < rec["quarantined_until"] <= now:
                # the moment the ban ended, not the moment it was noticed: expiry is seen on the
                # next poll, and anything the site failed in between is evidence about the site
                # after its ban, which must still count
                rec["cleared_at"] = rec["quarantined_until"]
                rec["quarantined_until"] = 0.0
                self._dirty = True

    def _baseline(self, site):
        """(jobs, failure rate) of every *other* site.

        The baseline has to exclude the site under test: a black hole that has eaten most of the
        production would otherwise dominate the baseline and excuse itself.
        """
        n = n_fail = 0
        for name, rec in self.sites.items():
            if name == site:
                continue
            a, b = self._counts(name, rec)
            n += a
            n_fail += b
        return n, ((n_fail / n) if n else 0.0)

    def _quarantine_seconds(self, rec):
        """How long this site's next quarantine lasts: the base, doubled once per previous one."""
        # the exponent is clamped only so that an absurd count cannot overflow the multiplication;
        # 2**20 base durations is already many times the ceiling
        doublings = min(int(rec["quarantines"]), 20)
        hours = float(self.cfg["quarantine_hours"]) * 2.0**doublings
        return min(hours, float(self.cfg["max_quarantine_hours"])) * 3600.0

    def _quarantine(self, now):
        for site, rec in self.sites.items():
            if rec["quarantined_until"] > now:
                continue
            n, n_fail = self._counts(site, rec, since=rec["cleared_at"])
            if not n or n_fail < int(self.cfg["min_failures"]):
                continue
            rate = n_fail / n
            if rate < float(self.cfg["min_failure_rate"]):
                continue
            n_other, rate_other = self._baseline(site)
            if n_other < int(self.cfg["min_baseline_jobs"]):
                continue
            if rate < float(self.cfg["relative_factor"]) * rate_other:
                continue
            rec["quarantined_until"] = now + self._quarantine_seconds(rec)
            rec["quarantines"] += 1
            self._dirty = True
