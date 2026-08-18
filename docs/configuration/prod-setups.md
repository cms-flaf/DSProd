# Production setups

A **production setup** is a single YAML file that describes one production: which process, which
eras, which NanoAOD versions, and which points. Every task takes it via `--setup <path>`.

Setups are **model-dependent**, so they live with their model in the
[DSProdModels](https://github.com/cms-flaf/DSProdModels) submodule, under
`<process>/setups/`. For X_HH:

```
models/X_HH/setups/Run3_XHHbbWW.yaml
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
| `nano_versions` | Per-era list of NanoAOD versions. A `default:` key supplies a fallback; a plain list applies to all eras. |
| `first_step` / `last_step` | Bound the CMSSW chain `RunProd` runs. |
| `events_per_job` | Events per `RunProd` seed (seeds per point and era = `ceil(events_total[era] / events_per_job)`). |
| `files_per_merge` | How many per-seed nanos `NanoMergeTask` groups into one output. |
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

Two options on **every** task make separate setups unnecessary:

```bash
# only the M-800 samples (both final states), full statistics
law run RunProd --setup <setup> --points '*_M-800'

# a 100-event end-to-end check of one sample, into a separate `<output>_test` area
law run RunProd --setup <setup> --points '*_M-800' --test 100
```

- `--points` takes fnmatch globs matched against point names (comma-separated for several). Output
  paths are keyed by era, point and seed — never by branch id — so a selective run writes exactly
  where the full production would, and the rest can be produced later.
- `--test <n>` produces `<n>` events per point and era in a single job, and redirects the products
  to `<output>_test` so a check can never overwrite a production sample. Gridpacks are exempt: they
  do not depend on the event count, so a test reuses the production ones.

Both are ordinary task parameters, so they propagate to the upstream tasks of the same run.

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
[DSProdGridpacks](https://github.com/cms-flaf/DSProdGridpacks) store follows from the process,
generator, energy and gridpack name, and then:

- **if the gridpack is present there**, `ImportGridpack` copies it to `fs_default`;
- **if it is absent**, `MakeGridpack` generates it.

So adding a gridpack to DSProdGridpacks makes the corresponding point use it; removing it makes
that point generate. Nothing in the setup changes either way.
