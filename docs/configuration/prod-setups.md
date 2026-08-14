# Production setups

A **production setup** is a single YAML file that describes one production: which process, which
eras, which NanoAOD versions, and which points. Every task takes it via `--setup <path>`.

Setups are **model-dependent**, so they live with their model in the
[DSProdModels](https://github.com/cms-flaf/DSProdModels) submodule, under
`<process>/setups/`. For X_HH_bbWW:

```
models/X_HH_bbWW/setups/Run3_XHHbbWW.yaml        # production
models/X_HH_bbWW/setups/Run3_XHHbbWW_test.yaml   # small end-to-end test
```

A setup is **backend-agnostic** — the same file runs with `--workflow local`, `htcondor`, or
`crab`. It carries **no** deployment or site settings (storage area, CRAB site, ...): those live
in the [global / user config](settings.md), so a shared model setup is not tied to one user.

## Fields

```yaml
process: X_HH_bbWW                        # registry key of the process (the plugin `name`)
conditions: config/conditions_Run3.yaml   # per-era conditions file (in DSProd, framework-level)

output: XHHbbWW                           # sub-directory under fs_default (user config)

eras: [ Run3_2022EE ]                     # eras to produce

nano_versions:                            # NanoAOD versions per era
  Run3_2022: [ v12, v15 ]
  Run3_2022EE: [ v12, v15 ]
  Run3_2023: [ v12, v15 ]
  Run3_2023BPix: [ v12, v15 ]
  Run3_2024: [ v15 ]

first_step: LHEGS                         # first production step
last_step: NANO                           # last production step

events_per_job: 2000                      # events per RunProd seed
files_per_merge: 25                       # per-seed nanos per NanoMergeTask group

points:
  - name: GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-800
    mass: 800
    spin: 0
    events_total: 100000
```

| Field | Meaning |
|---|---|
| `process` | Selects the [process](processes.md) (its registry `name`). |
| `conditions` | The [per-era conditions file](conditions.md) (a DSProd path — framework-level, shared). |
| `output` | Sub-directory under the user's storage area; products go to `<fs_default>/<output>` (see [settings](settings.md)). |
| `eras` | Eras to produce (must exist in the conditions file). |
| `nano_versions` | Per-era list of NanoAOD versions. A `default:` key supplies a fallback; a plain list applies to all eras. |
| `first_step` / `last_step` | Bound the CMSSW chain `RunProd` runs. |
| `events_per_job` | Events per `RunProd` seed (seeds per point = `ceil(events_total / events_per_job)`). |
| `files_per_merge` | How many per-seed nanos `NanoMergeTask` groups into one output. |
| `points` | The physics points; their exact shape is defined by the process. |

There is **no `gridpack:` field** and **no `crab:` block** — see below.

## Points and gridpacks

The `points` list is interpreted by the process's `enumerate_points`, so the recognised keys are
process-specific. For `X_HH_bbWW` each point has a `name`, `mass`, `spin`, `channel` (`SL` or `DL`)
and `events_total`.

A point does **not** name its gridpack. `MakeGridpack` derives the gridpack's canonical location
in the [DSProdGridpacks](https://github.com/cms-flaf/DSProdGridpacks) store from the process,
generator, energy and gridpack name, then:

- **if the gridpack is present there** (e.g. the M-800 gridpack committed via Git LFS) it is
  imported (materializing the LFS content on demand);
- **if it is absent** the gridpack-generation task runs automatically.

So adding a gridpack to DSProdGridpacks makes the corresponding point use it; removing it makes
that point generate. Nothing in the setup changes either way.

## Example setups (DSProdModels)

| Setup | Purpose |
|---|---|
| `X_HH_bbWW/setups/Run3_2023_XHHbbWW.yaml` | X→HH→bbWW SL+DL for Run3_2023 (44 samples, 6.3 M events). |
| `X_HH_bbWW/setups/Run3_2023BPix_XHHbbWW.yaml` | Same for Run3_2023BPix (44 samples, 3.4 M events). |
| `X_HH_bbWW/setups/Run3_2024_XHHbbWW.yaml` | Same for Run3_2024, covering 2024+2025+2026 (44 samples, 86.3 M events). |
| `X_HH_bbWW/setups/Run3_XHHbbWW.yaml` | Single-point production (M-800 SL, gridpack imported from DSProdGridpacks). |
| `X_HH_bbWW/setups/Run3_XHHbbWW_test.yaml` | Tiny end-to-end test (M-666, generated, 100 events). |

**One setup per era.** `events_total` is a property of a point, so a production whose statistics
scale with the era's luminosity needs its own setup per era — which also lets each era be launched,
and finish, independently.
