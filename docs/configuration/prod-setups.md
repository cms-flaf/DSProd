# Production setups

A **production setup** is a single YAML file that describes one production: which process, which
eras, which NanoAOD versions, and which points. Every task takes it via `--setup <path>`.

Setups are **model-dependent**, so they live with their model in the
[DSProdModels](https://github.com/cms-flaf/DSProdModels) submodule, under
`<process>/setups/`. A model ships one setup per signal — X_HH ships two, which share its cards
and gridpacks and differ in the gen fragment their points name and in the directory they write to:

```
models/X_HH/setups/Run3_XHHbbWW.yaml        # X -> HH -> bbWW,     output: XHHbbWW
models/X_HH/setups/Run3_XHHbbtautau.yaml    # X -> HH -> bbtautau, output: XHHbbtautau
```

**One setup covers every era, and the whole point list.** Producing part of it, or producing a few
events as a check, is a command-line option (`--points`, `--test`) — never another setup file.

A setup is **backend-agnostic** — the same file runs with `--workflow local`, `htcondor`, or
`crab`. It carries **no** deployment or site settings (storage area, CRAB site, ...): those live
in the [global / user config](settings.md), so a shared model setup is not tied to one user.

## Fields

```yaml
process: X_HH                             # registry key of the process (the plugin `name`)
conditions: config/conditions_Run3.yaml   # per-era conditions file (in DSProd, framework-level)

output: XHHbbWW                           # sub-directory under fs_default (user config)

eras: [ Run3_2023, Run3_2023BPix, Run3_2024 ]   # eras to produce

nano_versions:                            # NanoAOD versions per era
  Run3_2023: [ v12, v15 ]
  Run3_2023BPix: [ v12, v15 ]
  Run3_2024: [ v15 ]

first_step: LHEGS                         # first production step
last_step: NANO                           # last production step

events_per_job: 2000                      # events per RunProd seed
files_per_merge: 25                       # per-seed nanos per NanoMergeTask group

production_mode: GluGlutoRadion           # default production mode for the points below

resources:                                # what a job of this production asks the batch system
  RunProd:                                # for; a task left out keeps DSProd's own default
    max_runtime: 16                       # hours
    memory: 10000                         # MB per job
    n_cpus: 4                             # cores
  NanoMergeTask:
    max_runtime: 3
    memory: 5000
    n_cpus: 1
  MakeGridpack:
    max_runtime: 12                       # no `memory`: left to the framework
    n_cpus: 1

points:
  - name: GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-800   # the DAS dataset name
    mass: 800
    spin: 0
    final_state: 2B2JLNu                  # the gen fragment (DAS final-state token)
    events_total:                         # per era; an era left out produces nothing
      Run3_2023: 210000
      Run3_2023BPix: 120000
      Run3_2024: 2850000
```

| Field | Meaning |
|---|---|
| `process` | Selects the [process](processes.md) (its registry `name`). |
| `conditions` | The [per-era conditions file](conditions.md) (a DSProd path — framework-level, shared). |
| `output` | Sub-directory under the user's storage area; products go to `<fs_default>/<output>` (see [settings](settings.md)). |
| `eras` | Eras to produce (must exist in the conditions file). |
| `nano_versions` | Per-era list of NanoAOD versions. A `default:` key supplies a fallback; a plain list applies to all eras. `--nano-versions` narrows it per run (see [below](#running-part-of-a-setup)). |
| `first_step` / `last_step` | Bound the CMSSW chain `RunProd` runs. |
| `events_per_job` | Events per `RunProd` seed (seeds per point and era = `ceil(events_total[era] / events_per_job)`). |
| `files_per_merge` | How many per-seed nanos `NanoMergeTask` groups into one output. |
| `resources` | Per task, what its jobs ask the batch system for — `max_runtime` (hours), `memory` (MB per job) and `n_cpus` (cores). Optional; see [below](#what-a-job-asks-for). |

!!! warning "`events_total` must fill whole merged files"
    A sample is delivered as `events_total / (events_per_job * files_per_merge)` files, and DSProd
    refuses a setup where that does not divide — otherwise the last group of a point is short and
    the sample is N full files plus a stub, which makes a later top-up production awkward: "how
    many more files do I need" stops having a whole-number answer. Round `events_total` **up** to a
    multiple of the product. `--test` is exempt, since it deliberately runs a single short job.

    `events_per_job` belongs to the **setup**, not to a point: a point carrying its own value is
    refused, because the check above divides by the setup's number while `RunProd` produces the
    point's. `NanoMergeTask` re-checks the same contract per group from the `produced/` records
    before merging, so a group whose seeds were produced at different sizes is refused rather than
    merged into a file of the wrong size — the record's *requested* size is compared, because a
    product made before the per-step assertion existed can hold fewer than it asked for (40 of the
    166 merged Run3_2023 v12 files hold 49 998 or 49 999).
| `points` | The physics points; their exact shape is defined by the process. `events_total` is **per era** — a mapping `{era: n}`, or a scalar to use the same number everywhere. |

There is **no `gridpack:` field** and **no `crab:` block** — see below.

## Events per era

`events_total` is given **per era**, so a production whose statistics scale with each era's
luminosity still fits in one setup:

```yaml
    events_total:
      Run3_2023: 210000
      Run3_2023BPix: 120000
      Run3_2024: 2850000
```

A scalar is also accepted and means "this many in every era". An era that a point does not list
produces nothing for that point; an era that is not in `eras:` at all is an error, so a typo cannot
silently produce zero events.

## Running part of a setup

Four options on **every** task make separate setups unnecessary:

```bash
# one era of the full grid
law run RunProd --setup <setup> --eras Run3_2023

# only the M-800 samples (both final states), full statistics
law run RunProd --setup <setup> --points '*_M-800'

# a 100-event end-to-end check of one sample in one era, into a separate `<output>_test` area
law run RunProd --setup <setup> --eras Run3_2023 --points '*_M-800' --test 100

# only one NanoAOD version, halving what the era costs on storage
law run RunProd --setup <setup> --eras 'Run3_202[23]*' --nano-versions v12
```

- `--eras` takes fnmatch globs matched against the setup's `eras:` (comma-separated for several,
  e.g. `'Run3_2023*'` for 2023 and 2023BPix). Everything downstream follows: `InstallCMSSW` builds
  only the releases those eras need, and a point that produces no events in them drops out of the
  run, so its gridpack is not prepared either.
- `--points` takes the same kind of globs, matched against point names.
- `--test <n>` produces `<n>` events per point and era in a single job, and redirects the products
  to `<output>_test` so a check can never overwrite a production sample. Gridpacks are exempt: they
  do not depend on the event count, so a test reuses the production ones.
- `--nano-versions` picks among the versions the setup gives an era. It only ever **narrows**: a
  version the setup does not list for an era is not produced by asking for it. Each version is a
  full second copy of the era's events — measured on delivered files, 5.3 kB/event in v12 and
  6.7 kB/event in v15 — so dropping one is the largest storage lever a production has. Asking for
  a set that leaves a selected era with nothing is an error naming that era, on the same principle
  as a pattern that matches nothing.

A pattern that matches nothing is an error, never an empty run. Output paths are keyed by era,
point and seed — never by branch id — so a selective run writes exactly where the full production
would, and the rest can be produced later. All three are ordinary task parameters, so they
propagate to the upstream tasks of the same run, and each combination gets its own local job area
(law keys its control files by branch range).

## What a job asks for

How long a job runs, how much memory it needs and how many cores it can use are properties of the
*production*, not of DSProd: one model's gridpack takes minutes where another's takes hours, and a
denser era needs more memory per event than a light one. Each setup therefore carries its own
`resources:` block, keyed by task:

```yaml
resources:
  RunProd:
    max_runtime: 16     # hours; `16h`, `90m` and `16` are read as the command line reads them
    memory: 10000       # MB per job
    n_cpus: 4           # cores
```

Three settings are accepted per task — `max_runtime`, `memory` and `n_cpus` — and a task may name
any subset: **a setting the setup does not name is left to DSProd**, which is how you say "no
particular requirement". There is no second way to say it — a value that asks for nothing
(`memory: 0`) is refused, because zero runtime would mean *no limit* on CRAB and a negative one on
HTCondor, and a zero memory or core count is the framework's own marker for "work it out". Precedence is the one you would expect, because the block enters luigi's own
configuration layer rather than being pushed onto the task afterwards:

```
--RunProd-max-runtime 30h    >    the setup's resources:    >    DSProd's default for that task
```

So one run can override a setup without editing it, and a task the setup does not mention keeps the
default DSProd ships (`RunProd` 24 h / 10000 MB / 4 cores, `NanoMergeTask` 3 h / 5000 MB / 1 core,
`MakeGridpack` 12 h). An entry naming a task that does not exist, a setting that is not one of the
three, a value the setting cannot take, or a value that asks for nothing is **refused** when the
setup is read. So is a task that
does not run on a batch system (`ImportGridpack` runs locally, so it has no cores to ask for) and a
shared base class such as `HTCondorWorkflow`, which carries the three settings but is nobody's task:
a resource request nobody applies would otherwise be noticed only when the jobs came back killed.

!!! note "Naming a memory can cost you a core"
    CRAB sells memory only in per-core units — `max(3000, 2500 × numCores)` MB — so a request is
    rounded up to the cores that can hold it: `memory: 3000` for a single-core job buys it a
    **second core** it never uses, while a job that names no memory gets the same 3000 MB on one
    core. That is why the `MakeGridpack` entry above names none (see
    [settings](settings.md#cores-and-memory-are-asked-for-separately)). The same arithmetic is why
    `NanoMergeTask` above runs on two CRAB cores although `haddnano` is single-threaded: its 5000 MB
    needs them, and `n_cpus: 1` is still the right statement of what the payload uses.

Asking for less is not free in either direction. A job that outlives its `max_runtime` is killed;
but CRAB tests the request against the time a pilot has **left**, not against its full length, so a
shorter one matches more pilots and starts sooner. Which way to err is a judgement about that
production — and one worth writing down next to the number, since nothing else records it.

## Points and gridpacks

The `points` list is interpreted by the process's `enumerate_points`, so the recognised keys are
process-specific. For `X_HH` each point has a `name`, `mass`, `spin`, `final_state`,
`events_total`, and optionally `production_mode`.

**Names follow DAS.** A point is named after the central dataset it reproduces, and the two tokens
it sets are taken from that same name:

```
/GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-800/...
 └──────┬──────┘            └──┬───┘
  production_mode            final_state
```

- `final_state` selects the gen fragment `<comEnergy>/fragments/<final_state>.py`, so adding a
  final state to a model is adding a fragment;
- `production_mode` selects the cards `<comEnergy>/cards/<production_mode>/` and, with them, the
  gridpack naming — so a second production mode (VBF, …) is a second cards directory. It defaults
  to the setup-level `production_mode:`, which is why the points above do not repeat it.

A point does **not** name its gridpack. Its canonical location in the
[DSProdGridpacks](https://gitlab.cern.ch/cms-flaf/DSProdGridpacks) store follows from the process,
generator, energy and gridpack name, and then:

- **if the gridpack is present there**, `ImportGridpack` copies it to `fs_default`;
- **if it is absent**, `MakeGridpack` generates it.

So adding a gridpack to DSProdGridpacks makes the corresponding point use it; removing it makes
that point generate. Nothing in the setup changes either way.
