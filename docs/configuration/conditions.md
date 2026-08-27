# Era conditions

`config/conditions_Run3.yaml` holds everything that varies by era: CMSSW releases, `SCRAM_ARCH`,
global tags, beamspot, pileup inputs, and the list of production steps. The values are sourced
from the CMS McM public REST API, so a DSProd sample matches the corresponding central campaign.

`dsprod/run_step.py` reads this file to build the `cmsDriver` command for each step.

## Step resolution

The effective parameters for one step are the merge of four layers, later overriding earlier:

```
default  →  default_step[STEP]  →  <era>.default  →  <era>[STEP]
```

- **`default`** — values common to every era and step (e.g. `comEnergy: 13600`).
- **`default_step[STEP]`** — the standard `step`/`eventcontent`/`datatier` for each step
  (`LHEGS`, `DIGIPremixHLT`, `RECO`, `MINIAOD`, `NANO`).
- **`<era>.default`** — the era's baseline (`SCRAM_ARCH`, `CMSSW`, `GlobalTag`).
- **`<era>[STEP]`** — per-era, per-step overrides (e.g. MiniAOD often uses a newer release than
  GEN).

The final `NANO` step additionally carries a `versions:` map — one entry per NanoAOD version to
produce — and each version is resolved the same way on top of the `NANO` defaults. This is how a
single era can emit both `v12` and `v15` from the same MiniAOD.

## Example (excerpt)

```yaml
Run3_2022:
  prod_steps: [ LHEGS, DIGIPremixHLT, RECO, MINIAOD, NANO ]
  default:
    SCRAM_ARCH: el8_amd64_gcc10
    CMSSW: CMSSW_12_4_11_patch3
    GlobalTag: 124X_mcRun3_2022_realistic_v12
  MINIAOD:
    SCRAM_ARCH: el8_amd64_gcc11
    CMSSW: CMSSW_13_0_13
    GlobalTag: 130X_mcRun3_2022_realistic_v5
  NANO:
    versions:
      v12:
        CMSSW: CMSSW_13_0_13
        GlobalTag: 130X_mcRun3_2022_realistic_v5
      v15:
        SCRAM_ARCH: el8_amd64_gcc12
        CMSSW: CMSSW_15_0_15_patch4
        GlobalTag: 150X_mcRun3_2022_realistic_v1
```

## Which CMSSW gets installed

[`InstallCMSSW`](../concepts/tasks.md) collects the `(SCRAM_ARCH, CMSSW)` pairs across all
`prod_steps` (and every NanoAOD version) for an era and builds each one once. So the conditions
file is also the single source of truth for which releases a production needs.

!!! warning "`# VERIFY` fields"
    Values confirmed against a reachable McM chain are exact; those marked `# VERIFY` in the file
    are inferred by pattern and should be checked against that era's own McM chain before the first
    real production in the era.

## Process modifiers

`procModifiers` is usually set in `default_step`, which every era inherits. An era that must not
carry it sets `procModifiers: ""` — an empty value is treated as "none" and the flag is left off the
`cmsDriver` command entirely. Run3_2023 needs this: Run3Summer22 runs RECO with
`siPixelQualityRawToDigi`, Run3Summer23 does not, and applying it there makes every job fail with
`No data of type "SiPixelQuality" with label "forRawToDigi"`.
