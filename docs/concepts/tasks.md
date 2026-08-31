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
branch per selected era, and is idempotent — `env.sh` guards each release with a `.installed` flag. The
batch tasks require it so the releases exist before jobs run.

### `ImportGridpack` and `MakeGridpack`

Both provide the same product — `<output>/gridpacks/<gridpack-name>/gridpack.tar.xz`, relative to
`fs_default` — and both branch over **distinct** gridpacks, not points, since several points can
share one (the `2B2JLNu` and `2B2L2Nu` final states of a mass point use the same gridpack, and one
branch per point would make them race on the same output). Which of the two does the work depends only on where the
gridpack already is:

| state | what happens |
|---|---|
| already on `fs_default` | nothing — both tasks are complete |
| in the [DSProdGridpacks](https://gitlab.cern.ch/cms-flaf/DSProdGridpacks) store | **`ImportGridpack`** copies it to `fs_default` |
| nowhere | **`MakeGridpack`** generates it, and stages the result to `fs_default` |

The setup never names a gridpack: its location in the store is derived from the process's
`gridpack_rel_path`, which mirrors the model's own layout level for level
(`<process>/<generator>/<comEnergy>/<production_mode>/<gridpack-name>/`), and presence there alone
decides import vs. generate. The gridpack **name** carries the production mode
(`GluGlutoRadiontoHH_M-800`), because a production stores its gridpacks flat under
`<output>/gridpacks/`.

`ImportGridpack` is **always local**, and `MakeGridpack` requires it, so the import has happened
by the time `MakeGridpack` decides what to submit. That matters: reading the store needs the git
checkout and the Git-LFS server, which a grid worker has neither of — without this split, a batch
job would regenerate a gridpack the repository already holds. Under the sparse checkout the
tarball is not even in the working tree, so `ImportGridpack` streams the object from the LFS
server straight to `fs_default` and verifies it against the pointer's size and sha256.

`MakeGridpack` renders the point's cards and runs
`genproductions_scripts/bin/<generator>/gridpack_generation.sh`. It is the only one of the two
that is worth a batch backend (`--workflow htcondor|crab`).

!!! note "Clean environment for generation"
    `gridpack_generation.sh` sets up its own CMSSW and aborts if one is already active. DSProd
    strips the `CMSSW_*`/`SCRAM`/`PYTHON*` variables from the generation subprocess, so gridpack
    generation works even on a CRAB worker (where `env.sh` sets up a cvmfs CMSSW for LAW itself).

!!! warning "Never generated *inside* a production job"
    `RunProd` requires `MakeGridpack`, so a worker that finds the gridpack missing — often only
    because it cannot reach `fs_default` — would schedule it and spend the production slot on
    MadGraph, about 1.5 h, before failing to upload the result from that same worker.

    A `MakeGridpack` branch running on a batch node therefore checks *what was submitted*: if this
    job was launched for `MakeGridpack` itself, it generates normally — producing gridpacks on the
    grid is a supported thing to do, `law run MakeGridpack --workflow htcondor|crab`. If the job
    was launched for something else, it refuses and says so at once. That error means either the
    gridpack really is missing, in which case submit `MakeGridpack` for it, or `fs_default` is
    unreachable from the worker.


### `PremixFileList`

Resolves an era's premix pileup dataset to a plain file list on `fs_default`
(`<output>/premix/<era>.txt`), once, and `RunProd` passes it to `cmsDriver` as
`--pileup_input filelist:...`.

Without it, `--pileup_input dbs:<dataset>` makes **every job** resolve that dataset — ~38 000 files
— with its own DAS query. At a few thousand concurrent jobs the queries start returning nothing,
cmsDriver then writes a config with no secondary input, and cmsRun dies on

```
NoSecondaryFiles: RootEmbeddedFileSequence no input files specified for secondary input source
```

*after* the job has already produced its GEN-SIM. A 5000-job production lost 10 % of its jobs that
way. The list is identical to what a successful DAS query returns, so nothing about the physics
changes; it is stored in the production area (not `<output>_test`) because it depends only on the
era, and a batch node refuses to build one — that query belongs on the submitting machine.

### `RunProd`

The core production task: a fused GEN→…→MiniAOD→NanoAOD chain for one `(era, point, seed)`, run
via `cmsDriver` steps (`dsprod/run_step.py`). Branches are enumerated by
`runprod_branches(eras, points)` — the single source of truth for branch numbering, shared with
`NanoMergeTask`. For each requested NanoAOD version it stages one file and records that it did:

```
<output>/staging/nanoAOD_<version>/<era>/<point>/nano_<version>_<seed>.root
<output>/produced/nanoAOD_<version>/<era>/<point>/nano_<version>_<seed>.json
```

The **record**, not the staged file, is what `RunProd` declares as its output. `NanoMergeTask`
deletes each staged file once it has merged it, so the file's presence cannot say whether a seed
ran: a resumed workflow would find the outputs of every already-merged seed missing and regenerate
the whole era (law marks such jobs `initially missing task outputs`). Nothing deletes the records,
so completeness survives the merge. To redo a seed deliberately, delete its record along with its
nano file.

`RunProd` requires the VOMS proxy, `InstallCMSSW` (for its era), and `MakeGridpack` (for its
point). Its steps run `cmsDriver` with `--nThreads <n_cpus>` (2 by default), so
the job's core allocation is what cmsRun actually uses; a `nThreads` in the conditions overrides it
per step. The proxy requirement is satisfied by the batch-delegated proxy
inside a job (see [Grid proxy](../getting-started/installation.md#grid-proxy)). The number of seeds per point and era follows from
`events_total[era] / events_per_job` — `events_total` is per era, so one setup covers all of them.

### `NanoMergeTask`

Merges a group of per-seed nanos (`files_per_merge` per group) into one output with `haddnano`,
verifies that the merged event count equals the sum of the inputs, and — only then — removes the
staged per-seed inputs. It takes the input paths from the grouping itself, not from what `RunProd`
declares (that is the record), and refuses to run if a staged file of its group is gone although
its seed is recorded as produced. Output is the final, FLAF-facing file:

```
<output>/nanoAOD_<version>/<era>/<point>/nano_<version>_<group>.root
```

### `BackfillProducedRecords`

A migration for productions that ran before the `produced/` records existed, whose staged files
have already been merged away: it reconstructs the records from what is on storage — a merged file
accounts for the whole group of seeds behind it, a surviving staged file for its own seed — and
leaves existing records alone, so it is safe to re-run (delete its `backfill.done` flag to make it
list storage again).

```sh
law run BackfillProducedRecords --setup <setup> --eras Run3_2023 --workflow local
```

### `CollectGridpacks`

The way back: it collects the gridpacks a setup **produced** into the local
[DSProdGridpacks](https://gitlab.cern.ch/cms-flaf/DSProdGridpacks) checkout, so they can be committed
and reused (and so the next production imports them instead of regenerating them).

For each distinct gridpack of the setup it reports one of three states — already `in store`,
`collected` (it was on `fs_default`, so it is downloaded into the store checkout), or
`not produced` yet — and writes the same list to `data/CollectGridpacks/<setup>/collected.json`.
Every collected gridpack gets a `README.md` next to it recording how it was made: the setup, the
cards it came from, and the exact commits of DSProd, DSProdModels and `genproductions_scripts` —
the provenance the store requires.

It **commits nothing**. Adding ~30 MB of Git-LFS content per gridpack stays a deliberate act, so
the task ends by printing the commands to run:

```bash
law run CollectGridpacks --setup <setup>
# ... then, as printed:
git -C gridpacks add --sparse '<process>/<generator>/<comEnergy>/<production-mode>/<gridpack-name>'
git -C gridpacks commit -m "add ..."
git -C gridpacks push
```

`--sparse` is required: the store is [checked out sparsely](../getting-started/installation.md),
and a plain `git add` skips a `*.tar.xz` path with only a hint.

The task has no output to be "done": it re-runs every time it is asked for.

### `MakeManifest` (planned)

A future task that writes the dataset manifest FLAF uses to enumerate the produced samples.
Not yet implemented.

## Useful LAW options

- `--print-status -1` — show what LAW considers done vs. pending for the full graph, without
  running anything.
- `--print-deps -1` — print the dependency tree (with the backend each task would use).
- `--eras '<glob>'` / `--points '<glob>'` — produce only the matching eras / points of the setup
  (see [production setups](../configuration/prod-setups.md#running-part-of-a-setup)).
- `--test <n>` — produce `<n>` events per point and era in one job, into a separate `<output>_test`
  area.
- `--branch <n>` / `--branches <a,b>` — run only selected branches of a workflow.
- `--workers <n>` — run several branches in parallel locally.
- `--<TaskName>-<param> <value>` — override a parameter of an upstream task
  (e.g. `--MakeGridpack-workflow local` while `RunProd` runs on HTCondor).
