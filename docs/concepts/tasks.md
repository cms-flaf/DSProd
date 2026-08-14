# Tasks & LAW

DSProd is built on [LAW](https://github.com/riga/law) (the Luigi Analysis Workflow). You declare
*what* output you want by running a task; LAW resolves the dependency graph and runs only the
steps that are not already done. Every production task takes the same `--setup <path>` parameter
(the [production setup YAML](../configuration/prod-setups.md)) and a `--workflow`
[backend](backends.md).

The tasks live in `dsprod/tasks.py`.

## Task reference

### `InstallCMSSW`

Installs, once on the shared AFS area, the CMSSW releases an era's production needs (all
production steps plus every requested NanoAOD version). It is a **local** workflow with one
branch per era, and is idempotent — `env.sh` guards each release with a `.installed` flag. The
batch tasks require it so the releases exist before jobs run.

### `MakeGridpack`

Provides the gridpack for each point (one branch per point). It derives the gridpack's canonical
location in the [DSProdGridpacks](https://github.com/cms-flaf/DSProdGridpacks) store (from the
process's `gridpack_rel_path`) and then:

- **import** — if the gridpack is present there, copy it to the output (materializing the Git-LFS
  content on demand);
- **generate** — otherwise render the point's cards and run
  `genproductions_scripts/bin/<generator>/gridpack_generation.sh`.

The setup never names a gridpack — presence in DSProdGridpacks alone decides import vs. generate.

Output: `<output>/gridpacks/<gridpack-name>/gridpack.tar.xz`, relative to `fs_default`. It is keyed
by the gridpack name, which is channel-independent, so points sharing a gridpack share it.

!!! note "Clean environment for generation"
    `gridpack_generation.sh` sets up its own CMSSW and aborts if one is already active. DSProd
    strips the `CMSSW_*`/`SCRAM`/`PYTHON*` variables from the generation subprocess, so gridpack
    generation works even on a CRAB worker (where `env.sh` sets up a cvmfs CMSSW for LAW itself).

### `RunProd`

The core production task: a fused GEN→…→MiniAOD→NanoAOD chain for one `(era, point, seed)`, run
via `cmsDriver` steps (`dsprod/run_step.py`). Branches are enumerated by
`runprod_branches(eras, points)` — the single source of truth for branch numbering, shared with
`NanoMergeTask`. For each requested NanoAOD version it stages one file:

```
<output>/staging/nanoAOD_<version>/<era>/<point>/nano_<version>_<seed>.root
```

`RunProd` requires the VOMS proxy, `InstallCMSSW` (for its era), and `MakeGridpack` (for its
point). The number of seeds per point follows from `events_total / events_per_job`.

### `NanoMergeTask`

Merges a group of per-seed nanos (`files_per_merge` per group) into one output with `haddnano`,
verifies that the merged event count equals the sum of the inputs, and — only then — removes the
staged per-seed inputs. Output is the final, FLAF-facing file:

```
<output>/nanoAOD_<version>/<era>/<point>/nano_<version>_<group>.root
```

### `MakeManifest` (planned)

A future task that writes the dataset manifest FLAF uses to enumerate the produced samples.
Not yet implemented.

## Useful LAW options

- `--print-status -1` — show what LAW considers done vs. pending for the full graph, without
  running anything.
- `--print-deps -1` — print the dependency tree (with the backend each task would use).
- `--branch <n>` / `--branches <a,b>` — run only selected branches of a workflow.
- `--workers <n>` — run several branches in parallel locally.
- `--<TaskName>-<param> <value>` — override a parameter of an upstream task
  (e.g. `--MakeGridpack-workflow local` while `RunProd` runs on HTCondor).
