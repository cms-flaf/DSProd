# Driving a long production

A production is not a program that runs to completion on its own. The `law run` process — the
**driver** — is what polls CRAB, resubmits what failed, releases the next
[wave](../concepts/backends.md#job-waves) and schedules the merges. Everything it does happens
while it is alive, and nothing does while it is not.

That matters more than it sounds. In the Run3_2023BPix production of 4800 `RunProd` branches,
`data/jobs/` records **14 driver invocations over 7 days** — the driver dies roughly once a day —
and one of those gaps, with nothing polled between 09-01 16:20 and 09-03 14:12, accounts for
**27 h of the 68.4 h** the production took to reach 99.4 %. The jobs themselves were not the
problem: 4798 of 4800 started within half an hour of submission, and job execution took 7.1 h at
the median.

A dead driver is also invisible. The CRAB tasks keep running, jobs keep finishing, `crab status`
keeps answering — the production just stops moving.

## Run it under `drive.sh`

```bash
cd <production area>
source env.sh
run_tools/drive.sh law run RunProd --setup models/X_HH/setups/Run3_XHHbbWW.yaml --workflow crab
```

Anything after the options is the law command, run as given. `drive.sh` adds four things around it:

- **A log.** Everything, the driver's own messages included, goes to
  `data/logs/driver_<UTC stamp>.log` through `tee`, so an interactive operator still sees the run
  and a `nohup`'d one is not lost. A refusal is logged too.
- **A lock**, so two drivers cannot drive the same area (see below).
- **Restarts** with exponential backoff, but only for exit codes where running the same command
  again can help (see below).
- **A credential pre-flight** that refuses to start rather than run into an expiring proxy.

Useful options — `--help` lists them all, and each has an environment form:

| option | default | |
|---|---|---|
| `--max-restarts <n>` | 20 | consecutive restarts before giving up |
| `--min-proxy-hours <h>` | 24 | VOMS proxy lifetime below which it refuses to start |
| `--min-myproxy-days <d>` | 5 | MyProxy lifetime below which it refuses to start (CRAB only) |
| `--stale-lock-hours <h>` | 1 | age at which a lock whose process is gone may be taken over |

The restart budget counts *consecutive* restarts: a leg that ran for an hour or more was not a
crash loop, so it resets the budget and the backoff. A driver dying daily therefore never runs out,
while a command that fails in seconds stops after 20 attempts instead of hammering the CRAB server.

`SIGINT` (Ctrl-C) and `SIGTERM` stop the driver instead of restarting it, and are passed on to law.
Without that forward a `kill` looked like it did nothing at all: bash defers a trap until the
foreground command returns, so the driver would sit there for the remaining hours of the law run.

## The lock

`drive.sh` holds `data/driver.lock`, a directory created with `mkdir` — atomic, and unlike `flock`
dependable on the EOS area shared between the machines a production is driven from. It records the
host, pid, start time, log path and command:

```
$ cat data/driver.lock/owner
host lxplus912.cern.ch
pid 3487412
started_epoch 1788500000
started 2026-09-03T14:12:31Z
log /eos/.../DSProd/data/logs/driver_20260903T141231Z.log
command law run RunProd --setup models/X_HH/setups/Run3_XHHbbWW.yaml --workflow crab
```

A second driver prints what it found and then refuses — a live lock is never taken silently:

- **the recorded pid is alive here** → refused, a driver is already running;
- **the lock belongs to another host** → refused, because the pid in the record says nothing on
  this machine: it may be absent here and very much alive there. Check with `ps -p <pid>` on that
  host, and if it really is gone, remove the lock by hand (`rm -rf data/driver.lock`);
- **the pid is gone and the lock is older than `--stale-lock-hours`** → taken over, saying so;
- **the pid is gone but the lock is younger than that** → refused, because a lock that fresh is
  more likely half-written by a driver starting right now than abandoned.

The lock is released when the driver exits for any reason it can see, signals included.

## Credential clocks

The pre-flight runs before *every* law start, including a restart, and refuses when

- the **VOMS proxy** has less than 24 h left, or
- the **MyProxy delegation** the CRAB server renews job credentials from has less than 5 days left
  (checked only when the command asks for a `crab` workflow).

It prints the exact command to fix each case, and **never creates, renews or removes a credential
itself**. That is deliberate and not a convenience gap: `voms-proxy-init` cannot run unattended,
and `CreateVomsProxy.run()` removes the existing proxy *before* creating a new one — so an
automated restart that tried to renew would take away the only credential the production has and
get nothing back. A refusal leaves exactly the credential that was there.

```bash
voms-proxy-init --voms cms -rfc -valid 192:00 --out $X509_USER_PROXY
myproxy-init -d -n -s myproxy.cern.ch          # verify: myproxy-info -d -s myproxy.cern.ch
```

A 192 h proxy outlives a 24 h threshold by a week, so in practice this fires on the production that
has already been running for a week — exactly where an expiring proxy would otherwise turn into
every remote call failing at once.

## What law's exit code means

law leaves luigi's patched return codes in place (`law/patches.py`), and luigi reports the **most
severe** condition of the run, not all of them: with a scheduling error *and* a failed task the
code is 50. `drive.sh` classifies them:

| code | meaning | driver |
|---|---|---|
| 0 | the requested task is complete | done |
| 40 | `task_failed` — a task failed | **resume** (how a long production normally ends a leg) |
| 30 | `not_run` — the root task did not run to completion | **resume** |
| 20 | `missing_data` — an external input was missing | **resume** (storage unreachable?) |
| 50 | `scheduling_error` — `complete()`/`requires()` raised | **resume** (a failed listing, or a bug) |
| 10 | `already_running` — another worker holds the task | stop |
| 60 | `unhandled_exception` — an internal error before any task ran | stop |
| 1 | law aborted: unknown task family, unimportable module, bad command line | stop |
| 137 | `SIGKILL` — the node killed law (out of memory, a login-node limit) | **resume** |
| 130 / 143 | `SIGINT` / `SIGTERM` — someone stopped the run | stop |

!!! warning "Never set `task_failed: 1`"
    A `[luigi_retcode] task_failed: 1` in `law.cfg` collapses "some branches failed, resume the
    production" into the same `1` that a typo in the command line returns. The classification above
    — the only thing that lets a restart be automatic and safe — is gone the moment that option is
    set, for this driver and for any other supervision built later.

## The staleness alarm

`drive.sh` cannot report its own death, so the alarm lives outside it:

```bash
run_tools/check_driver_alive.py --area <production area>
```

It prints nothing and exits 0 while a driver is polling; when the production has stalled it prints
one report to stderr and exits 1. `--help` shows a ready-to-paste acron line — acron, so that the
check does not run on the machine whose death it is meant to report, and mails whatever is printed:

```
acrontab -e
*/30 * * * * lxplus.cern.ch <checkout>/run_tools/check_driver_alive.py --area <production area>
```

The liveness signal is the newest `data/*/*/crab_jobs_*.json`. law rewrites that dump on every poll
iteration, so its mtime is the one piece of driver state visible from any machine that can read the
area. The name carries the workflow's branch range and there is one per store directory, so it is
found by globbing rather than by name, and a read that catches it half-written is treated as a
driver at work, not as a fault.

The default threshold is **45 minutes** — deliberately not a multiple of `poll_interval` (5 min).
Measured gaps between polls in the live production are 5.3 min at the median, 12.1 min at p90 and
**32.3 min at most**, because submitting a wave is silent for 5–13 min at a time and a failed
status query skips the dump for that iteration. A threshold of twice the poll interval alarms on a
perfectly healthy production, and an alarm that cries wolf is not read.

Two situations are silence, not health, and are treated as such:

- **nothing to drive** — an area with no CRAB project directory under `data/jobs/` never submitted
  to the grid, so there is no driver to miss;
- **a finished production** — when the newest dump says every job is `finished` and none are
  waiting, its mtime only gets older and means nothing.

A job the dump calls `failed` is **work**, not an end. law keeps its retry counts per process
(`_job_retries`, `law/workflow/remote.py`) and never reads them back from the dump, so a restarted
driver hands every failed branch a full retry budget and resubmits it after one polling iteration.
"Everything finished except a handful of failures, and nobody driving" is what a black-hole site
produces at the tail of a production — where a stall costs the last percent of the sample — so it
is reported. Retries parked by the wave gate sit in `unsubmitted_jobs` and count as work for the
same reason: a production can read as all-finished and still be owed a wave.

An area that has CRAB project directories but no dump at all is reported too: nothing there has
ever recorded a poll. law keeps its job directories for ever (`job_file_dir_cleanup: False`), so
clearing a store directory to restart clean leaves exactly that.

Every report is made **once**, not every 30 minutes — the "no dump" one included. What was reported
is kept in `data/logs/driver_alarm_state.json`; deleting that file re-arms the report. If the driver
comes back, polls once and dies again, the mtime has changed and the new stall is reported. A
production that has genuinely ended with permanent failures mails once and then stays quiet,
because its dump mtime never changes again.

The alarm covers the CRAB backend, which is what a multi-day production uses; an HTCondor
production writes `htcondor_jobs_*.json` and has no project directories to key on.

!!! note "Supervision was the largest term, not the only one"
    The same 68.4 h also contains ~10.5 h of retries held back by the wave gate (median parking
    11.35 h) and four serialised retry generations of genuinely long jobs. Keeping the driver alive
    does not shorten those, and 169 of 192 merge groups being complete with none merged is a
    scheduling question in `NanoMergeTask`, not a supervision one.
