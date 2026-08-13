# Production setups

A **production setup** is a single YAML file (in `config/prod_setups/`) that fully describes one
production: which process, which eras, which NanoAOD versions, and which points. Every task takes
it via `--setup <path>`. The path is resolved relative to the repository root (or absolute).

## Fields

```yaml
process: X_HH_bbWW                       # registry key of the process module
conditions: config/conditions_Run3.yaml  # per-era conditions file

storage: /eos/user/k/kandroso/DSProd/XHHbbWW   # output root (change to your EOS area)

eras: [ Run3_2022EE ]                    # eras to produce

nano_versions:                           # NanoAOD versions per era
  Run3_2022: [ v12, v15 ]
  Run3_2022EE: [ v12, v15 ]
  Run3_2023: [ v12, v15 ]
  Run3_2023BPix: [ v12, v15 ]
  Run3_2024: [ v15 ]

first_step: LHEGS                        # first production step
last_step: NANO                          # last production step

events_per_job: 2000                     # events per RunProd seed
files_per_merge: 25                      # per-seed nanos per NanoMergeTask group

points:
  - name: GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-800
    mass: 800
    spin: 0
    events_total: 100000
    gridpack: /cvmfs/.../Radion_hh_narrow_M800_..._tarball.tar.xz
```

| Field | Meaning |
|---|---|
| `process` | Selects the [process module](processes.md) (its registry `name`). |
| `conditions` | The [per-era conditions file](conditions.md). |
| `storage` | EOS root for all products. Set this to your own area. |
| `eras` | List of eras to produce (must exist in the conditions file). |
| `nano_versions` | Per-era list of NanoAOD versions. A `default:` key can supply a fallback list; a plain list applies to all eras. |
| `first_step` / `last_step` | Bound the CMSSW chain `RunProd` runs. |
| `events_per_job` | Events per `RunProd` seed. The number of seeds per point is `ceil(events_total / events_per_job)`. |
| `files_per_merge` | How many per-seed nanos `NanoMergeTask` groups into one output. |
| `points` | The physics points. Their exact shape is defined by the process module. |
| `crab` | Optional [CRAB backend](../concepts/backends.md) settings. |

## Points

The `points` list is interpreted by the process module's `enumerate_points`, so the recognised
keys are process-specific. For `X_HH_bbWW` each point has a `name`, `mass`, `spin`, and
`events_total`, plus an **optional** `gridpack`:

- **with `gridpack:`** — import that existing tarball (central gridpacks live on cvmfs);
- **without `gridpack:`** — generate the gridpack from the process
  [cards template](processes.md).

The `name` is the canonical storage name and becomes the `<point>` directory in the
[storage layout](../concepts/architecture.md#storage-layout).

## Example setups

The repository ships several setups under `config/prod_setups/`:

| Setup | Purpose |
|---|---|
| `Run3_XHHbbWW.yaml` | Full production (imports the central M-800 gridpack). |
| `Run3_XHHbbWW_test.yaml` | Tiny end-to-end test (M-666, generate mode, 100 events). |
| `Run3_XHHbbWW_gpval.yaml` | Gridpack-generation validation. |
| `Run3_XHHbbWW_crabtest.yaml` | CRAB submission test (includes a `crab:` block). |
