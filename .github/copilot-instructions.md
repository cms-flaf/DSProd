# DSProd — instructions for Copilot code review

A LAW-based tool for custom CMS Monte-Carlo production: gridpack → LHE → GEN-SIM → DIGI → AOD →
MiniAOD → NanoAOD, driven from a declarative setup and run on the grid. It shares its design and
much of its machinery with [FLAF](https://github.com/cms-flaf/FLAF) but produces samples rather
than analysing them.

**Read `FLAF/.github/copilot-instructions.md` first.** Its sections on law semantics, remote
storage and concurrency describe machinery that exists here too (`dsprod/law_gfal.py`,
`dsprod/tasks.py`), as do its rules on what a useful review comment looks like and what not to
flag. This file adds what is specific to production.

## What costs the most here

A production runs for days across many sites and produces samples that analyses then trust. The
expensive failures are the ones that finish successfully with the wrong content: the wrong
conditions global tag, the wrong number of events, a step run with a mismatched CMSSW release. No
downstream task will notice — the files are valid, they are simply not what was asked for.

Weigh a diff by what it would cost to discover the mistake after the samples exist.

## Invariants

### Conditions and campaign settings

`config/conditions_Run3.yaml` pins the global tags and campaign parameters each era is produced
with. It is validated by the `conditions-check` workflow, which also runs on a schedule because
**central campaigns get amended after they open**. A change here is a change to the physics
content of every sample produced afterwards: check that the era being edited is the one intended,
and that a value copied from another era is deliberate rather than a fill-in.

### The gridpack store must never be checked out normally

`gridpacks` (`cms-flaf/DSProdGridpacks` on CERN GitLab) is a Git-LFS store checked out **sparsely,
with LFS downloads disabled**, by `setup_gridpacks.sh`. A plain `git submodule update --init`
pulls roughly 610 MB of LFS content. Flag any script, workflow, documentation snippet or CI step
that adds `gridpacks` to a recursive submodule update, or that does not go through
`setup_gridpacks.sh`.

### Step chaining

`dsprod/run_step.py` drives the per-step `cmsDriver` invocations. Each step's output is the next
step's input, and each runs in its own CMSSW release. Look for: a step whose release or
conditions do not match what the setup declares, an output filename that does not thread through
to the next step, and event counts that change across the chain without a filter to explain it.

### Job and storage machinery

`dsprod/law_gfal.py`, `law_wlcg.py`, `grid_tools.py` and `tasks.py` mirror FLAF's. The FLAF
invariants apply: a task's completeness is decided by paths on remote storage, remote writes are
not immediately visible, and `exists()` results are cached. A production task that concludes
"already done" from a stale or partial path skips real work silently.

Two rules were bought expensively and must not be relaxed. **A listing that fails is not an empty
directory**: `gfal_ls_checked` treats only gfal's own *no such file or directory* as absence and
raises on anything else, so no decision about a product is ever made from an endpoint that merely
could not be reached. And **a batch status does not retire a branch**: CRAB reports a failed
payload as `transferring`, which law maps to finished, so `crab_check_job_completeness` makes law
verify the products first — and drops the cached listings as it does, so the verdict rests on a
listing newer than the status it judges. Code that reintroduces either shortcut books work that was never done.

CRAB's `Site.storageSite` / `Data.outLFNDirBase` are the fixed placeholder `_CRAB_PLACEHOLDER_STAGEOUT`
in `dsprod/crab.py`, never derived from configuration or the environment (`fs_default`, `$USER`). CRAB
transfers nothing for DSProd, but the REST server refuses a `/store/user/<name>/` LFN whose `<name>` is
not the submitter, and the task worker resolves the pair to a Rucio write PFN at the site
(T3_CH_CERNBOX maps `/store/user/` only): a value built from either input fails for some submitter or
some site. The source of each check is cited at the constant.

law's `poll()` fixes the job count (backlog included) once at its start, and law's `submit()` drops
backlog jobs whose branches already exist; on a fresh run such a job is also in `jobs`, recorded
finished by luigi's scheduling (`process_resources`). `DSProdCrabWorkflowProxy.submit` skips the
round when law's tree is unreadable, so `poll()` drops those jobs first (`_drop_complete_backlog`):
otherwise the loop waits for a job that does not exist. Code that changes the backlog between
`run()` and `poll()` has to keep that count right.
The same skipped round parks the retries law offers (`_park_retries`, as the wave gate does), after
the mass-retry brake has judged them: a retry left in `jobs` keeps its old CRAB job id and every poll
reads it failed again, spending one of its retries.

### Resource requests come from three layers

`max_runtime`, `memory` and `n_cpus` are resolved by luigi in one order: the command line, then the
production setup's `resources:` block, then the task's own default. Code that reads one of them
from anywhere else — a value patched onto the task after it is built, a second copy of a default —
breaks that order silently, and the symptom is a job killed on walltime or memory rather than an
error. On CRAB the memory number is a kill threshold, not a reservation, so a request quietly
lowered (or raised, which buys cores) is a dead branch.

### `produced/` records are the completeness signal

A seed's `produced/` record, not its nano file, is what says the seed ran: the merge deletes the
staged files it consumed, so a production judged by files reads as entirely unproduced after a
merge. Two rules follow, and code that breaks either is expensive rather than wrong-looking. A
record must never be written before the file it describes is on storage, or a merge will trust a
file that is not there. And deleting a record means re-producing that seed from scratch, so any
code path that removes one must prove nothing accounts for it — a merged file covers its whole
group, and a listing that *failed* is not an empty listing. `exists()` answers by listing the
parent and caching the result, so that distinction has to survive the listing itself: it does
because `gfal_ls_checked` raises rather than returning "empty" when it cannot reach the endpoint.

### Pinned installs

Everything `env.sh` installs with `pip` is pinned in one list, `dsprod_pins` in `env.sh` (pip
first, then the packages it installs into `soft/dsprod_env` and `soft/vendor`), and the
`unit-tests` workflow installs the same pins; `test/test_env_install_pins.py` checks both. The pins
change together with the code that needs them: `dsprod/crab.py` overrides law's CRAB job manager
and its status parsing, which change between releases without an error (law 0.1.21 renamed `proxy`
to `proxy_file` and began answering `WAITING on command SUBMIT` and `SUBMITFAILED` itself). Flag a
diff that pins or installs a package of the list anywhere else, that changes a pin in `env.sh` or
CI alone, or that adds a per-package probe or version check — in `env.sh`, at import, or as a
version branch or `getattr`/`try` fallback that keeps another release working. Each of the two
directories records the list in its marker `.installed`, written last, and is rebuilt from scratch
when the marker is missing or records another list. Whether each directory exists is decided once,
by `_dsprod_installed` from a listing of `soft/`, and its removal and replacement follow that answer
alone; flag a second existence check (`[ -e ]`, `[ -d ]`) on either directory outside
`_dsprod_installed`, or a match on a listing whose failure reads as "not listed". A listing,
lookup or marker that fails or disagrees with itself stops `env.sh` and is never taken for a stale
build. Inside a law batch job
(`LAW_JOB_HOME` set) `env.sh` never builds or rebuilds either directory, and `soft/vendor` is
filled and marked in a sibling directory and moved into place whole; flag a diff that lets a job
build either, or that runs `pip --target` straight into `soft/vendor`. The steps that must succeed
before either directory counts as finished check their own exit status rather than run through
`run_cmd`, which does not stop a shell that ignores SIGINT (any `cmd &` from a script); flag a diff
that moves one back.

### Registry and processes

`dsprod/registry.py` and `dsprod/processes/` map a setup onto the steps that implement it. A
process added to the registry without the corresponding setup entry — or the reverse — fails only
when someone tries to produce it.

## Documentation must ship with the change

A PR must update the documentation **in the same PR** whenever it changes anything a user can
observe: a task or its arguments, a configuration key in `config/global.yaml`,
`config/conditions_Run3.yaml`, `config/user_custom.yaml` or a production setup, a CLI flag, an
output or log location, the installation or environment steps, a CI workflow, or a default.

This repository has its own MkDocs site under `docs/` —
`concepts/{architecture,tasks,backends}.md`, `configuration/{conditions,processes,prod-setups,settings}.md`,
`getting-started/{installation,first-production}.md`. New pages must be wired into `nav:` in
`mkdocs.yml`, and the build is verified with `mkdocs build --strict`. A change to what a
production setup may declare belongs in `configuration/prod-setups.md` or
`configuration/processes.md`; a change to conditions belongs in `configuration/conditions.md`.

If the change alters what `cms-flaf/DSProdModels` must provide, say so — that is a separate
repository and needs its own PR.

## Do not flag

- Shelling out to `cmsDriver.py`, `cmsRun` or CRAB tooling; that is the interface CMS provides.
- The vendored copies of FLAF-like helpers (`law_gfal.py`, `grid_tools.py`) — deliberate, so a
  production does not depend on an analysis framework.
- Missing unit tests for code that needs CVMFS, CMSSW or a grid proxy.
- Formatting — the `formatting-check` workflow settles it.

## Repository facts

Verified 2026-08-27 (layout and pins 2026-10-09); re-check before relying on any of it.

| | |
|---|---|
| Layout | `dsprod/` (`tasks.py`, `run_step.py`, `registry.py`, `config.py`, `crab.py`, `gridpack_store.py`, `law_gfal.py`, `law_wlcg.py`, `grid_tools.py`, `site_stats.py`, `watchdog.py`, `tools.py`, `processes/`), `config/`, `run_tools/` (`check_conditions.py`, `apply_format.sh`), `docs/`, `env.sh`, `setup_gridpacks.sh` |
| Pins | `dsprod_pins` in `env.sh`: pip 26.0.1, luigi 3.7.3, law 0.1.21; the same in the `unit-tests` workflow, which runs Python 3.12 (see "Pinned installs") |
| Submodules | `models` (`cms-flaf/DSProdModels`), `genproductions_scripts` (CERN GitLab), `gridpacks` (`cms-flaf/DSProdGridpacks`, **sparse LFS — use `setup_gridpacks.sh`**) |
| Configs | `config/global.yaml`, `config/conditions_Run3.yaml`, `config/law.cfg`. `config/user_custom.yaml` is the per-user override, layered on top of `global.yaml` and **gitignored** — a PR must never add it |
| Workflows | `unit-tests`, `formatting-check`, `repo-sanity-checks`, `conditions-check` (also scheduled weekly), `deploy-docs` |
| Docs | `docs/` with `mkdocs.yml`; verify with `mkdocs build --strict` |
