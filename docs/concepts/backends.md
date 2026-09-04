# Backends

Every production task accepts `--workflow <backend>`. The three backends differ only in *where*
the jobs run — the task logic, and all remote EOS I/O, are identical. DSProd always writes its
products to the production's storage area (see [settings](../configuration/settings.md)) itself, so the batch systems are used purely for
compute.

| `--workflow` | Runs on | Use for |
|---|---|---|
| `local` | the current machine | tests, small productions, debugging |
| `htcondor` | CERN HTCondor batch | medium productions |
| `crab` | the WLCG grid (CRAB) | large productions needing many sites |

## `local`

Runs branches in the current shell. Combine with `--workers <n>` to run several branches in
parallel. Needs a valid VOMS proxy for the EOS writes. This is the backend used by the
[first-production walkthrough](../getting-started/first-production.md).

## `htcondor`

Submits to the CERN HTCondor pool. Jobs bootstrap from the shared AFS checkout (`bootstrap.sh`)
and see the installed CMSSW releases under `soft/` directly. Relevant knobs (all `significant=False`,
so they do not change task identity):

- `--max-runtime <hours>` (per-task defaults: MakeGridpack 12 h, RunProd 24 h, NanoMergeTask 3 h);
- `--n-cpus <n>` (RunProd defaults to 2); cmsDriver runs its steps with that many threads;
- `--krenew <hours>` — how often to renew the Kerberos ticket while polling.

`--max-runtime` and `--n-cpus` are **per task**: each task keeps its own default and neither value
is passed on to the tasks it requires. Running `NanoMergeTask` therefore still gives its `RunProd`
requirement 24 h and 4 CPUs, not the merge task's 3 h and 1 CPU. To change one, address the task by
name — `--RunProd-max-runtime 36`.

Jobs request AlmaLinux9 workers and write their HTCondor logs under `data/logs/`.

## Resuming a production (both batch backends)

When a run picks up an existing submission, law re-checks the outputs of every job it had recorded
as finished and retries the ones whose outputs are missing (`initially missing task outputs`). For
a few files deleted by hand that is exactly what you want. For a whole production it is not, so a
check that condemns more than 10 % of the workflow stops the run instead, without submitting
anything:

```
8299 of 8300 jobs recorded as finished no longer have their outputs, so this run would
regenerate most of the sample. Nothing was submitted.
```

What to look at, in order:

1. **Were the outputs consumed downstream?** `NanoMergeTask` deletes each nano file it merges, and
   the [`produced/` records](tasks.md#runprod) are what marks those seeds done. Check that the
   records exist — for a production that predates them, run `BackfillProducedRecords`.
2. **Was the storage reachable?** A listing that fails looks the same from here. Run again once it
   is back.
3. **Do you actually want the work redone?** Delete the workflow's submission file under
   `data/jobs/` and start again — a run with nothing to resume never performs this check.

## `crab`

Submits to the WLCG grid via [CRAB](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideCrab),
built on `law.contrib.cms.CrabWorkflow`. This is the backend for large-scale private production,
where CERN HTCondor alone does not provide enough resources.

A production on this backend runs for days, and the `law run` process driving it has died about
once a day in practice, taking polling, resubmission and merging with it. Start it under
[`run_tools/drive.sh`](../operations/long-productions.md) and watch it with
`run_tools/check_driver_alive.py`.

Because WLCG workers have **no AFS**, the DSProd code (plus `genproductions_scripts` and the
vendored `law`/`luigi`) is shipped as a CRAB `inputFiles` tarball, built at submit time and
unpacked by `bootstrap.sh`; CMSSW is set up from cvmfs on the worker. DSProd owns all output and
log I/O (products go to the production's storage area via the gfal-CLI interface), so CRAB's own
stageout and log transfer are forced off.

The tarball is built once per `law run` and checked before it is used: every top-level entry must
be present and the archive readable. GNU tar's "file changed as we read it" warning — routine when
the production area sits on EOS and harmless, since the entry is still archived in full — is
tolerated rather than allowed to abort a submission of thousands of jobs.

**Gridpacks are not part of that tarball** — the input sandbox is size-limited, and a ~30 MB
gridpack per job would be wasteful anyway. A production job downloads the gridpack it needs from
`fs_default`, where [`ImportGridpack` or `MakeGridpack`](tasks.md) put it. The `gridpacks/` store
is likewise never shipped: importing from it is local by construction.

### Requirements

- a shell with `env.sh` sourced — it puts DSProd's `crab` wrapper and `python` shim on `PATH`,
  both of which law needs to drive CRAB (see the note below);
- a VOMS proxy **and** a MyProxy credential valid for at least 5 days (see
  [Installation](../getting-started/installation.md));
- optionally, a `crab:` block in the [global / user config](../configuration/settings.md) — **not**
  in the production setup, so the same setup runs on any backend:

```yaml
crab:
  max_memory_mb: 2500
  max_cores: 4          # ceiling on a job's cores; caps each task's own n_cpus
  # parallel_jobs: 5000     # jobs per CRAB task / in flight
  # refill_fraction: 0.2    # min wave size / free slots, as a fraction of parallel_jobs
  # retry_release_minutes: 45  # release a parked retry after this long, whatever the wave size
```

!!! note "No CRAB output location"
    The `crab:` block holds **compute settings only**. Products go to `fs_default` — the same
    location as with any other backend. CRAB does demand a `storageSite`/`outLFNDirBase` even when
    it transfers nothing; DSProd fills those in automatically as a submit-time formality, so there
    is nothing to configure and no second storage area to keep in sync.

### Job waves

A production of tens of thousands of branches cannot be one CRAB task (a task holds at most a few
thousand jobs). DSProd therefore keeps at most `crab.parallel_jobs` jobs in flight — 5000 by
default — and submits the rest in waves. So a 43 000-branch production is simply launched as one
`law run`; there is no need to chunk the branch range by hand. `--parallel-jobs <n>` on the command
line overrides both settings.

A wave becomes its own CRAB task only when it is worth one. `crab.refill_fraction` (default 0.2)
sets that bar as a fraction of `parallel_jobs`: with the defaults a wave needs **1000 jobs waiting
and 1000 free slots**. Jobs below the bar are held back — but only for as long as reaching it is
still possible. Once the work left in the whole production (running + waiting) can no longer fill
a wave, waiting could only delay it, so whatever is waiting goes out at once, however little that
is. `parallel_jobs` set to unlimited (`--parallel-jobs 0`) bypasses all of this and restores law's
own behaviour.

What is measured against the bar is the **waiting backlog** — never-submitted branches plus the
retries already held back. A generation of retries offered by the current poll is parked first and
counted on the next one, so a fresh generation never opens the gate on size before it has waited
for anything (it still counts towards the tail rule above, which is about what the production can
still fill, not about what has waited).

Three consequences worth knowing:

- **Small productions are never batched.** A 12-job production can never fill a wave, so a job that
  fails there is resubmitted on the next poll, exactly as before.
- **Large productions have a short tail, not a serialised one.** Retries are held only while the
  production is still busy; they are released as soon as fewer than one wave of work remains, not
  when the last job finishes. A 3270-job production that loses 226 jobs early runs as two CRAB
  tasks, with the retries going out around three quarters of the way through.
- **A parked retry is released on a timer** after `crab.retry_release_minutes` (default 45),
  whatever the wave size. Waiting for a wave that a handful of retries cannot fill costs a full
  job length per retry generation: over the 4800-branch Run3_2023BPix production the parked
  retries waited **11.35 h at the median**, ~10.5 h of the 68.4 h it took to reach 99.4 %. The
  window is per parked *set*, not per job: a release restarts it for whatever it could not take,
  so a production losing jobs in a trickle creates at most one extra CRAB task per window (226
  failures over those 68.4 h would be ~1 task per 45 min holding a couple of jobs each). Set
  `retry_release_minutes: 0` to let every retry out on the next poll, or a value larger than any
  production takes (e.g. `100000`) to switch the timer off and go back to the size bar alone —
  which is worth knowing if `refill_fraction: 1.0` was set to insist on full waves.

Only the waiting *time* lives in the driver's memory; the parked jobs themselves are in the
submission file, next to their attempt counts. A restarted driver therefore finds them, starts a
fresh window on its first polling iteration and releases them one window later — at worst one
extra window of waiting, never a lost job.

Once a wave does go out, law fills it up to `parallel_jobs` from the backlog whatever opened the
gate — so a release on the timer takes as many never-submitted branches with it as there are free
slots. The CRAB task is being created either way, and the parked retries are at the front of the
queue, so they are in it. A never-submitted branch never starts a window of its own: the backlog
alone still waits for a full-sized wave.

Without the size bar, law creates a fresh CRAB task the moment a single job finishes or fails —
that 3270-job production produced a second, 226-job CRAB task ten minutes in.

### Site selection

DSProd jobs carry **no real input dataset** (they generate their own events, and the CRAB config
sets `ignoreLocality`), so nothing ties them to a particular site. Because `ignoreLocality` is set,
the CRAB client **requires** a `Site.whitelist`; DSProd therefore defaults it to every tier
(`T1_*`, `T2_*`, `T3_*`), which is the widest pool the client accepts. Configuring a `whitelist`
can only narrow it, so do not add one just to "get more sites".

Restricting is worth it in two situations:

- **Sites that cannot reach your storage.** Unlike a normal CRAB task, DSProd does its own stageout:
  the job writes its product over the WAN to `fs_default` (e.g. CERNBox via `davs://`) instead of to
  local site storage. A worker without outbound access to that endpoint will run the full payload
  and only then fail on the copy. Gridpack *generation* likewise needs outbound network to fetch
  generator tarballs. Put such sites in `blacklist` as you find them.
- **Keeping jobs near the storage**, e.g. `whitelist: [ T2_CH_CERN ]` for short tests against
  CERNBox — convenient for debugging, but it throttles throughput, so avoid it for real production.

CMS's **global blacklist** of known-broken sites stays in force; `ignore_global_blacklist: true`
waives it, which is not recommended with an open site pool.

!!! warning "whitelist entries must be processing sites"
    If you do set a `whitelist`, every entry must be a genuine CMS **processing** site: do **not**
    put a storage-only site (e.g. `T3_CH_CERNBOX`) there, or CRAB refuses the submission with
    "not in the list of known CMS Processing Site Names".

### Failing sites

One broken worker node fails jobs in seconds, frees its slot and takes the next one, so it can eat
a large share of a production before anyone notices — on 2026-08-27 a single host at one T2 failed
258 jobs with `/usr/bin/base64: Input/output error`, before any physics ran.

DSProd therefore keeps its own record of how jobs fare per site in
`data/crab_site_stats.json` and quarantines a site that is clearly misbehaving. CRAB reports where
each job ran, and since every wave is a new CRAB task, the next wave — retries included — is
submitted without the quarantined sites. It is on by default; the thresholds live under
`crab.auto_blacklist`:

| key | default | meaning |
|---|---|---|
| `enabled` | `true` | `auto_blacklist: false` keeps only the static `blacklist` |
| `min_failures` | 5 | failures needed before a site can be quarantined at all |
| `min_failure_rate` | 0.5 | ... and the fraction of the jobs *sent* there that failed |
| `relative_factor` | 2.0 | ... and how many times worse than the other sites it must be |
| `min_baseline_jobs` | 20 | ... judged against at least this many jobs elsewhere |
| `quarantine_hours` | 6 | how long it stays out; afterwards its record starts clean |
| `window_hours` | 24 | outcomes older than this stop counting |
| `max_sites` | 10 | never quarantine more than this many sites at once |

Only what CRAB says about a job enters the record, and only from the status response itself: a
job that finished, or that failed **with a job-level error code**. Everything else is law's own
bookkeeping and says nothing about a site — a resumed run flips jobs whose outputs are gone to
retry, and a killed task reports all of its jobs as failed. Counting those once turned a single
poll into 8285 failures spread over every site of the production, and the ~100 % baseline that
resulted left the quarantine unable to fire for a node that was failing two thirds of its jobs.
If a record ever looks like that, delete `data/crab_site_stats.json` — it is advisory and rebuilds
within a poll or two.

A site's failure rate counts every job **sent** there — the ones that already ended plus the ones
still in flight. That distinction matters more than it looks: a job fails in seconds and succeeds in
hours, so a rate computed over finished jobs alone reads as ~100 % at every site early in a
production, no site stands out, and nothing is ever quarantined. Counting jobs in flight, the site
that swallowed 335 of its 391 jobs sits at 0.86 while everyone else is between 0.007 and 0.08.

The last four defaults are what keep this from making things worse. A site is only quarantined for
being **worse than the others**, judged against a real baseline, so a bug of your own — which fails
everywhere — blacklists nothing; a lone site is never quarantined, because there would be nowhere
left to run; at most `max_sites` are held out at once; and every quarantine expires, after which the
site starts from a clean record rather than staying condemned. Jobs already submitted keep going to
the site they were assigned — CRAB cannot re-target a running task.

A site you know is bad belongs in the static `blacklist` instead: that one is never lifted.

!!! note "An unreadable status response does not stop the production"
    `crab status` occasionally returns output with no `Status on the CRAB server` line, and law
    treats that as a query error — one per *job* of the task, because a group query maps a single
    failure onto every job in it (4763 in one production poll). law then skips the rest of that poll
    entirely: no status line, no resubmission, and any other task's good data discarded with it.

    DSProd retries such a response three times, 15 s apart. If it still cannot be read, the task's
    jobs are reported as **pending** — what law itself does for a freshly submitted task with no
    per-job information yet — and the fact is printed once for the task instead of once per job. A
    task whose status stays unreadable for ten consecutive polls does raise: a production that
    quietly stalls is worse than one that stops.

!!! note "CRAB does not write to your AFS home"
    CRAB rewrites its task cache `~/.crab3` on *every* command, status queries included. With
    `$HOME` on AFS that makes a long production depend on an AFS token: when the token lapses,
    every status query fails at once with
    `PermissionError: [Errno 13] Permission denied: '/afs/.../.crab3.<pid>'` and law reports it as
    a status-query failure for all jobs. The `crab` wrapper `env.sh` installs therefore points
    `HOME` at `$DSPROD_CRAB_HOME` (default: a per-user directory under `$TMPDIR`), so nothing in a
    production run needs AFS. law passes `--proxy` to submit, status and kill, so CRAB never needs
    `~/.globus` from the real home either.

    DSProd still renews Kerberos and the AFS token (`kinit -R` + `aklog`, hourly, from
    `crab_poll_callback`) — but renewal can only extend a ticket that is still valid, so it is not
    a substitute for keeping the production off AFS.

!!! note "Why `env.sh` matters for CRAB"
    law runs `crab` inside a CMSSW sandbox of its own, and dumps that sandbox's environment with
    bare `python` — which modern CMSSW no longer ships, and for which the DSProd venv's `python`
    is not a working substitute under a `cmsenv`. `env.sh` therefore writes a `crab` wrapper and a
    `python` → `python3` shim into `soft/bin` and prepends it to `PATH`. Submitting from a shell
    that never sourced `env.sh` fails the sandbox, and law reports it only indirectly, as every
    job carrying `dummy_job_id` and being retried with `error: unknown job id`. DSProd now checks
    the sandbox before submitting and reports that case directly.

### Debugging CRAB jobs

`crab status`/`crab getlog` re-delegate a MyProxy interactively. To inspect a job without that,
fetch its stdout directly from the task's web directory with your VOMS proxy — remember
`--capath /etc/grid-security/certificates`, or curl returns HTTP 000. The
[CRAB backend module](https://github.com/cms-flaf/DSProd/blob/main/dsprod/crab.py) documents the
details.
