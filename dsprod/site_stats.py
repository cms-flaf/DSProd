"""Rolling per-site job statistics and dynamic CRAB site blacklisting.

A single broken worker node fails jobs in seconds, frees its slot and picks up the next one, so one
bad host can eat a large share of a production: on 2026-08-27 `comp-g-001.local` at T2_EE_Estonia
failed 258 jobs with `/usr/bin/base64: Input/output error`, before any physics ran. CRAB accepts a
blacklist only per *site* and only at submission time, so DSProd keeps its own record in the
production area and quarantines a site whose recent jobs mostly fail. Since every wave is a new
CRAB task, the next wave -- retries included -- is submitted without it.

Configured under `crab.auto_blacklist` in the merged global config; see `DEFAULTS`.
"""

import json
import os
import time

#: `crab.auto_blacklist` settings and their defaults
DEFAULTS = {
    # set false to keep only the statically configured `crab.blacklist`
    "enabled": True,
    # a site needs at least this many failures before it can be quarantined at all
    "min_failures": 5,
    # ... and at least this fraction of its jobs in the window must have failed
    "min_failure_rate": 0.5,
    # ... and it must be failing this many times more often than the other sites, so a bug of our
    # own -- which fails everywhere -- cannot blacklist every site that runs it
    "relative_factor": 2.0,
    # ... judged against at least this many jobs elsewhere. Without a baseline the first site to
    # collect `min_failures` would be quarantined on its own record alone, before there is anything
    # to compare it with; with a single site there is also nowhere else to send the work.
    "min_baseline_jobs": 20,
    # how long a quarantine lasts; afterwards the site starts from a clean record
    "quarantine_hours": 6.0,
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
        self._dirty = False
        self.load()

    # -- persistence ------------------------------------------------------------------------

    def load(self):
        try:
            with open(self.path) as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        sites = data.get("sites")
        if isinstance(sites, dict):
            self.sites = {
                name: {
                    "events": [
                        (float(t), int(ok)) for t, ok in (rec.get("events") or [])
                    ],
                    "quarantined_until": float(rec.get("quarantined_until") or 0.0),
                }
                for name, rec in sites.items()
                if isinstance(rec, dict)
            }

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

    def record(self, site, ok, now=None):
        """Note one finished (`ok=True`) or failed job at `site`."""
        if not site:
            return
        now = time.time() if now is None else now
        rec = self.sites.setdefault(site, {"events": [], "quarantined_until": 0.0})
        rec["events"].append((float(now), int(bool(ok))))
        self._dirty = True
        self._prune(now)
        self._quarantine(now)

    # -- blacklisting -----------------------------------------------------------------------

    def blacklist(self, now=None):
        """The sites to keep out of the next submission, worst first."""
        if not self.cfg["enabled"]:
            return []
        now = time.time() if now is None else now
        self._expire(now)
        active = [
            (name, rec)
            for name, rec in self.sites.items()
            if rec["quarantined_until"] > now
        ]
        # most failures first, so the cap keeps the worst offenders
        active.sort(key=lambda item: -self._counts(item[1])[1])
        return [name for name, _ in active[: int(self.cfg["max_sites"])]]

    # -- internals --------------------------------------------------------------------------

    @staticmethod
    def _counts(rec):
        n = len(rec["events"])
        n_fail = sum(1 for _, ok in rec["events"] if not ok)
        return n, n_fail

    def _prune(self, now):
        cutoff = now - float(self.cfg["window_hours"]) * 3600.0
        for rec in self.sites.values():
            kept = [(t, ok) for t, ok in rec["events"] if t >= cutoff]
            if len(kept) != len(rec["events"]):
                rec["events"] = kept
                self._dirty = True

    def _expire(self, now):
        """Lift quarantines that have run out, and let the site start over."""
        for rec in self.sites.values():
            if 0.0 < rec["quarantined_until"] <= now:
                rec["quarantined_until"] = 0.0
                rec["events"] = []
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
            a, b = self._counts(rec)
            n += a
            n_fail += b
        return n, ((n_fail / n) if n else 0.0)

    def _quarantine(self, now):
        for site, rec in self.sites.items():
            if rec["quarantined_until"] > now:
                continue
            n, n_fail = self._counts(rec)
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
            rec["quarantined_until"] = (
                now + float(self.cfg["quarantine_hours"]) * 3600.0
            )
            self._dirty = True
