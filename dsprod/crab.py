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
      max_memory_mb: 2500
      max_cores: 1
      # whitelist: [ ... ]       # optional; default = every tier (T1_*, T2_*, T3_*)
      # blacklist: [ ... ]       # optional; exclude sites that fail to reach the storage
      # parallel_jobs: 5000      # jobs per CRAB task / in flight; --parallel-jobs wins
      # refill_fraction: 0.2     # min wave size / free slots, as a fraction of parallel_jobs
      # retry_release_minutes: 45  # a parked retry goes out after this long, whatever the wave
"""

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

#: minimum size of a wave, as a fraction of `parallel_jobs`, and the number of slots that must be
#: free to take it. Without it law submits a fresh CRAB task as soon as a single job finishes or
#: fails, producing hundreds of tiny tasks.
_CRAB_DEFAULT_REFILL_FRACTION = 0.2

#: how long a retry held back by the wave gate may wait before it goes out on its own, whatever
#: the wave size. Waiting for a wave that a handful of retries cannot fill costs a full job length
#: (7.1 h at the median) per retry generation: over a 4800-job production the parked retries
#: waited 11.35 h at the median, ~10.5 h of the 68.4 h it took to reach 99.4 %.
_CRAB_DEFAULT_RETRY_RELEASE_MINUTES = 45


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
        #: job ids already recorded, and the in-flight counts per project
        self._stats_seen = set()
        self._stats_lock = threading.Lock()
        self._in_flight = {}

    @classmethod
    def parse_query_output(cls, out, proj_dir, job_ids, skip_transfers=False):
        """Parse a status response, and say what it looked like when that fails.

        law's error names the server status it ended up with ("but got 'None'") but never the
        output it read, so an unreadable response cannot be diagnosed after the fact. Attach the
        head of it -- the status lines live in the first few lines, and the per-job JSON that
        follows is megabytes, so a slice is enough.
        """
        try:
            return super(DSProdCrabJobManager, cls).parse_query_output(
                out, proj_dir, job_ids, skip_transfers=skip_transfers
            )
        except Exception as exc:
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

    #: in-flight counts of a project not queried for this long stop counting
    in_flight_stale_seconds = 3600.0

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
            except Exception as exc:
                last_error = exc
                if attempt < self.query_retries:
                    time.sleep(self.query_retry_delay)
                continue
            self._unreadable.pop(proj_dir, None)
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
        if job_ids is None:
            job_ids = self._job_ids_from_proj_dir(proj_dir)
        return {
            job_id: self.job_status_dict(job_id=job_id, status=self.PENDING)
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
    def _wait_for_law_sources(cls):
        """Fail with the actual reason when law's own tree cannot be read.

        With the software on EOS a submission can hit a moment when it is not there, and the
        error that surfaces then is a copy of a path that never existed. Waiting for the tree
        turns a transient outage into a delay, and a real one into a message that names it.
        """
        base = os.path.dirname(os.path.abspath(law.contrib.cms.job.__file__))
        for rel in cls.law_sources:
            path = os.path.join(base, rel)
            for attempt in range(cls.source_retries + 1):
                if os.path.isfile(path):
                    break
                if attempt < cls.source_retries:
                    time.sleep(cls.source_retry_delay)
            else:
                raise RuntimeError(
                    f"{path} is not readable, so no CRAB job file can be built. law's own "
                    "tree is unreachable -- if it sits on EOS, the mount is likely down."
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


#: CRIC's site table — the same source CRAB validates a whitelist against
_CRIC_URL = "https://cms-cric.cern.ch/api/cms/site/query/?json"

#: how long a cached site list is reused before CRIC is asked again
_CRIC_CACHE_SECONDS = 24 * 3600


def processing_sites(cache_path=None, url=_CRIC_URL, timeout=60):
    """CMS site names that actually run jobs, newest-first from CRIC, cached on disk.

    `/cvmfs/cms.cern.ch/SITECONF` cannot be used for this: it also lists storage endpoints such as
    `T1_US_FNAL_Disk` and `T3_CH_CERNBOX`, and a whitelist naming one gets the task refused --
    "A site name T1_US_FNAL_Disk that user specified is not in the list of known CMS Processing
    Site Names". CRIC marks the difference: a site that runs jobs has `computeunits`.
    """
    if cache_path and os.path.exists(cache_path):
        if time.time() - os.path.getmtime(cache_path) < _CRIC_CACHE_SECONDS:
            try:
                with open(cache_path) as f:
                    return json.load(f)
            except (OSError, ValueError):
                pass
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            payload = json.load(response)
    except Exception as exc:
        if cache_path and os.path.exists(cache_path):
            with open(cache_path) as f:  # stale is better than nothing
                return json.load(f)
        raise RuntimeError(f"could not read the CMS site list from {url}: {exc}")
    entries = payload.values() if isinstance(payload, dict) else payload
    sites = sorted(
        e["name"] for e in entries if e.get("name") and e.get("computeunits")
    )
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
        """
        if _cli_has_parallel_jobs():
            return
        cfg_n = self.task._crab_cfg().get("parallel_jobs")
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
        min_myproxy_seconds = 5 * 24 * 3600
        for encode in (False, True):
            try:
                info = (
                    law.wlcg.get_myproxy_info(encode_username=encode, silent=True) or {}
                )
            except Exception:
                info = {}
            if info.get("username") and info.get("timeleft", 0) >= min_myproxy_seconds:
                kwargs["myproxy_username"] = info["username"]
                return kwargs
        raise RuntimeError(
            "CRAB requires a MyProxy credential valid for >= 5 days (the CRAB server "
            "retrieves it from myproxy.cern.ch). Run once:\n"
            "  myproxy-init -d -n -s myproxy.cern.ch\n"
            "  # verify: myproxy-info -d -s myproxy.cern.ch  (timeleft >= 5 days)"
        )


class CrabWorkflow(law.cms.CrabWorkflow):
    """CRAB remote workflow mixin for DSProd tasks.

    A production of tens of thousands of branches is submitted as a series of CRAB tasks of
    `crab.parallel_jobs` jobs each (see `DSProdCrabWorkflowProxy`), so no manual chunking of the
    branch range is needed.
    """

    workflow_proxy_cls = DSProdCrabWorkflowProxy
    poll_interval = luigi.FloatParameter(default=5.0, significant=False)

    #: lazily-built, throttled `kinit -R` used while polling (see crab_poll_callback)
    _crab_kerberos_update = None

    #: code tarball shipped to the workers, built once per law process (see _code_tarball)
    _code_tarball_path = None

    #: rolling per-site job statistics, fed by the job manager (see harvest_site_stats)
    _site_stats_obj = None

    crab_memory = luigi.IntParameter(
        default=-1,
        significant=False,
        description="max memory per CRAB job in MB; -1 = auto",
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

        return get_global().get("crab", {}) or {}

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
        # a large CRAB production polls for days, while law keeps writing its job status files to
        # the AFS work area — renew the Kerberos ticket as the HTCondor backend does
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
        return True

    def site_stats(self):
        """Rolling per-site job record, kept in the production area across runs."""
        if self._site_stats_obj is None:
            self._site_stats_obj = SiteStats(
                os.path.join(self.ana_data_path(), "crab_site_stats.json"),
                self._crab_cfg().get("auto_blacklist"),
            )
        return self._site_stats_obj

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
        n_cpus = max(1, int(getattr(self, "n_cpus", 1) or 1))
        mem = int(self.crab_memory)
        if mem <= 0:
            mem = int(self._crab_cfg().get("max_memory_mb", n_cpus * 2500))
        mb_per_core = int(self._crab_cfg().get("max_memory_mb_per_core", 2500))
        max_cores = int(self._crab_cfg().get("max_cores", 8))
        n_cores = max(n_cpus, (mem + mb_per_core - 1) // mb_per_core)
        n_cores = max(1, min(n_cores, max_cores))
        mem = max(mem, n_cores * mb_per_core)
        # the CRAB client refuses a task above max(5000, 2500 * numCores) MB, so clamp instead of
        # letting a generous `max_memory_mb` (with a low `max_cores`) fail the whole submission
        mem = min(mem, max(5000, 2500 * n_cores))

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
            processing_sites(os.path.join(self.ana_data_path(), "cms_sites.json")),
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
