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

`RunProd` carries its own [failure budget](../operations/long-productions.md#the-failure-budget)
(`retries: 3`, `tolerance: 0.05`, `acceptance: 1.0`) rather than law's, so one dead branch cannot
end a multi-day production while a short sample still fails the workflow.

`RunProd` requires the VOMS proxy, `InstallCMSSW` (for its era), and `MakeGridpack` (for its
point). Its steps run `cmsDriver` with `--nThreads <n_cpus>` (2 by default), so
the job's core allocation is what cmsRun actually uses; a `nThreads` in the conditions overrides it
per step. The proxy requirement is satisfied by the batch-delegated proxy
inside a job (see [Grid proxy](../getting-started/installation.md#grid-proxy)). The number of seeds per point and era follows from
`events_total[era] / events_per_job` — `events_total` is per era, so one setup covers all of them.

!!! warning "Never generated *inside* a merge job"
    `NanoMergeTask` requires single `RunProd` branches, so a merge branch whose seed has no
    record would make luigi run this 7 h chain inside the merge job — a 3 h slot with one core,
    which it loses on walltime. `RunProd` therefore makes the same check `MakeGridpack` does: on
    a batch node, a job launched for anything other than `RunProd` refuses to generate and names
    the seed it was asked for. Either that seed really is missing — submit `RunProd` for it, or
    merge only the groups [`merge_status.py`](#which-groups-can-be-merged) calls ready — or
    `fs_default` is unreachable from the worker, which is the other way a record looks missing.

### `NanoMergeTask`

Merges a group of per-seed nanos (`files_per_merge` per group) into one output with `haddnano`,
verifies that the merged event count equals the sum of the inputs, and — only then — removes the
staged per-seed inputs. It takes the input paths from the grouping itself, not from what `RunProd`
declares (that is the record), and refuses to run if a staged file of its group is gone although
its seed is recorded as produced. Output is the final, FLAF-facing file:

```
<output>/nanoAOD_<version>/<era>/<point>/nano_<version>_<group>.root
```

A group waits for **its own seeds only**, not for the generation stage as a whole: the workflow
requires exactly the `RunProd` branches of the groups it is running, taken from its branch map
*after* `--branches` has been applied. A group whose 50 seeds are on storage can therefore be
merged while the other 4750 of the era are still being produced. The requirement used to be the
entire `RunProd` workflow, and that is how the Run3_2023BPix production reached 169 of its 192
groups complete with not one merged.

Two consequences to know about:

- Asking for the whole merge (no `--branches`) requires every `RunProd` branch, which law
  collapses back to "all branches" — so a production driven through `NanoMergeTask` polls the
  same job data as `law run RunProd` itself, `data/RunProd/<store>/<backend>_jobs_0To<n>.json`. A
  **narrowed** merge run instead gets its own job-data file in that same directory, named after
  the branch ranges it requires (`crab_jobs_0To51_100To251.json`). Two drivers in one area would
  then submit the same seeds twice under two sets of job ids, which is what `drive.sh`'s
  [lock](../operations/long-productions.md#the-lock) is for. Neither the store directory nor any
  product path depends on the selection.
- `--branches` on the merge selects **merge groups** and asks for the seeds behind them. It used
  to hand the merge's own branch numbers to `RunProd` as if they were seeds, so `--branches 5`
  waited on `RunProd` branch 5 rather than on the 50 seeds of group 5.

#### Which groups can be merged

```sh
run_tools/merge_status.py --setup <setup> --eras Run3_2023BPix
```

Reports every merge group of the selection as one of

| state | meaning |
|---|---|
| `merged` | the merged file is on storage; nothing left to do |
| `ready` | every seed has a `produced/` record **and** its staged nano file |
| `blocked` | at least one seed has no record yet — `RunProd` still owes it |
| `broken` | every seed is recorded but a staged file is gone and the group is not merged |

and prints the `law run NanoMergeTask … --branches <ranges>` line that merges exactly the ready
ones, which is what makes merging part of a production a supported procedure rather than folklore.
It exits 0 normally and 1 when a group is `broken` — the state `NanoMergeTask` refuses to run on
(its merged file was removed after the fact); delete the affected seeds' `produced/` records so
they are produced again. `--all` lists every unmergeable group instead of the first 20, and the
same `--eras` / `--points` / `--test` selectors as the tasks take apply, since the branch numbers
it prints are only valid for the same selection.

It reads storage with three directory listings per (era, point, nano version) — the merged files,
the records, the staged nanos — `--threads` of them at a time (16 by default), never a stat per
seed: a full era is thousands of seeds per version, and at one remote round trip each the stats
alone run for hours. Because a failed listing and a missing directory are indistinguishable at
that layer, a report that finds *nothing at all* says so and points at the endpoint and the proxy
rather than claiming the production is unproduced.

### `BackfillProducedRecords`

A migration for productions that ran before the `produced/` records existed, whose staged files
have already been merged away: it reconstructs the records from what is on storage — a merged file
accounts for the whole group of seeds behind it, a surviving staged file for its own seed — and
leaves existing records alone, so it is safe to re-run (delete its `backfill.done` flag to make it
list storage again).

```sh
law run BackfillProducedRecords --setup <setup> --eras Run3_2023 --workflow local --workers 8
```

One branch per (era, point, nano version). Each branch reads the storage with three directory
listings rather than a stat per seed, and uploads its records `--upload-threads` at a time
(16 by default): a full era is 8300 seeds per nano version, and at one remote round trip per seed
the migration takes hours instead of minutes. Raise `--workers` and `--upload-threads` on a slow
endpoint — the work is all latency, not CPU.

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
