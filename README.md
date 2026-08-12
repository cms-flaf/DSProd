# DSProd

Law-based custom MC production for the FLAF ecosystem: **gridpack → NanoAOD** for Run3
eras (2022–2024), generic over process type. Each physics process ships a customization
module; gridpacks can be supplied (existing) or generated. Output NanoAOD feeds FLAF
directly via the private-nano (HLepRare) convention.

## Status

**Phase 1 (skeleton).** The generic framework is in place; the concrete production tasks
are added in later phases (see the architecture doc). Current pieces:

```
env.sh / bootstrap.sh / config/law.cfg   environment + law setup (CMSSW installed on demand)
genproductions_scripts/                  submodule (GitLab cms-gen) — gridpack generators
dsprod/
  tools.py        minimal utilities vendored from FLAF/RunKit (ps_call, voms proxy, kerberos)
  cmsEnv.sh        run a command inside a CMSSW runtime
  tasks.py         Task base + HTCondorWorkflow (+ kerberos renewal)
  registry.py      process-module registry
  processes/
    base.py        ProcessCustomization ABC + Point / GridpackSpec
```

## Production chain (design)

```
MakeProdCard ─► MakeGridpack ─► RunProd(era,point,seed) ─► NanoMergeTask ─► MakeManifest ─► FLAF
 (generate)   (or import existing)  │ fused GEN→DRPremix→MiniAOD→{NANOv12,NANOv15}, stage per-seed
                                    └─ NanoMergeTask haddnano's a group and drops the staged inputs
```

## Quick start

```bash
source env.sh          # sets up law; per-era CMSSW is installed on demand by the tasks
law index              # list available tasks
```

Full architecture and the McM-sourced per-era conditions: see the FLAF_all design doc
`CLAUDE/reviews/2026-08-12_dsprod-mc-production-architecture.md`.
