# DSProd

Law-based custom MC production for the FLAF ecosystem: **gridpack → NanoAOD** for Run3
eras (2022–2024), generic over process type. Each physics process ships a customization
module; gridpacks can be supplied (existing) or generated. Output NanoAOD feeds FLAF
directly via the private-nano (HLepRare) convention.

**Documentation:** see `docs/` (MkDocs Material). Build/preview with a throwaway venv:
`python3 -m venv /tmp/mkdocs_env && /tmp/mkdocs_env/bin/pip install mkdocs-material && /tmp/mkdocs_env/bin/mkdocs serve`.
Code style is checked in CI by the **Formatting Check** workflow (black + yamllint); run it
locally with `bash run_tools/apply_format.sh` before committing.

## Status

The framework and the core tasks are in place (MakeGridpack, RunProd, NanoMergeTask,
InstallCMSSW), runnable **local / HTCondor / CRAB**. CRAB has been validated end-to-end
(an M-800 gridpack generated on a grid worker and staged to EOS). Remaining: MakeManifest
(Phase 7). Current pieces:

```
env.sh / bootstrap.sh / config/law.cfg   environment + law setup (CMSSW installed on demand)
genproductions_scripts/                  submodule (GitLab cms-gen) — gridpack generators
dsprod_models/                           submodule (cms-flaf/DSProdModels) — model plugins + cards + fragments
dsprod_gridpacks/                        submodule (cms-flaf/DSProdGridpacks, Git LFS) — stored gridpacks
dsprod/
  tasks.py         Task base + HTCondorWorkflow + InstallCMSSW/MakeGridpack/RunProd/NanoMergeTask
  crab.py          CRAB backend (law.contrib.cms.CrabWorkflow); ships code via inputFiles
  run_step.py      cmsDriver step builder (GEN→…→NANO), per-step CMSSW
  registry.py      process registry (imports dsprod_models to discover plugins)
  processes/base.py  ProcessCustomization ABC (models themselves live in dsprod_models)
  tools.py         utilities vendored from FLAF/RunKit (ps_call, voms proxy, kerberos, retries)
  grid_tools.py / law_gfal.py / law_wlcg.py
                   FLAF's gfal-CLI remote-file interface (works on WLCG workers, where the
                   gfal2 python module law.contrib.gfal needs is unavailable)
```

## Production chain (design)

```
MakeGridpack ─► RunProd(era,point,seed) ─► NanoMergeTask ─► MakeManifest ─► FLAF
(generate or   │ fused GEN→DRPremix→MiniAOD→{NANOv12,NANOv15}, stage per-seed
import existing)└─ NanoMergeTask haddnano's a group and drops the staged inputs
```

## Quick start

```bash
source env.sh          # sets up law; per-era CMSSW is installed on demand by the tasks
law index              # list available tasks
```

## Running (backends)

Every production task accepts `--workflow local|htcondor|crab`. A production is described by a
setup YAML in `config/prod_setups/` (process, eras, nano versions, points, storage path).

```bash
# local test
law run MakeGridpack --setup config/prod_setups/Run3_XHHbbWW_test.yaml --workflow local

# HTCondor (CERN batch)
law run RunProd --setup <setup>.yaml --workflow htcondor

# CRAB (WLCG grid) — needs a VOMS proxy + a MyProxy credential valid >= 5 days
law run MakeGridpack --setup config/prod_setups/Run3_XHHbbWW_crabtest.yaml --workflow crab
```

CRAB notes: DSProd owns all output/log I/O (products go to the setup's `storage:` EOS path via
the gfal-CLI interface), so CRAB's own stageout/logs are forced off; the `crab:` block in the
setup (`storage_site`, `out_lfn_base`, optional `whitelist`) is only a submit-time write check
and the processing-site choice. WLCG workers have no AFS, so the code is shipped in the CRAB
`inputFiles` tarball and law/luigi are vendored (`soft/vendor`, built once by `env.sh`) so the
worker needs no PyPI.

Full architecture and the McM-sourced per-era conditions: see the FLAF_all design doc
`CLAUDE/reviews/2026-08-12_dsprod-mc-production-architecture.md`.
