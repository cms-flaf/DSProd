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

Verified 2026-08-27; re-check before relying on any of it.

| | |
|---|---|
| Layout | `dsprod/` (`tasks.py`, `run_step.py`, `registry.py`, `config.py`, `crab.py`, `gridpack_store.py`, `law_gfal.py`, `law_wlcg.py`, `grid_tools.py`, `site_stats.py`, `processes/`), `config/`, `run_tools/` (`check_conditions.py`, `apply_format.sh`), `docs/`, `env.sh`, `setup_gridpacks.sh` |
| Submodules | `models` (`cms-flaf/DSProdModels`), `genproductions_scripts` (CERN GitLab), `gridpacks` (`cms-flaf/DSProdGridpacks`, **sparse LFS — use `setup_gridpacks.sh`**) |
| Configs | `config/global.yaml`, `config/conditions_Run3.yaml`, `config/law.cfg`. `config/user_custom.yaml` is the per-user override, layered on top of `global.yaml` and **gitignored** — a PR must never add it |
| Workflows | `formatting-check`, `repo-sanity-checks`, `conditions-check` (also scheduled weekly), `deploy-docs` |
| Docs | `docs/` with `mkdocs.yml`; verify with `mkdocs build --strict` |
