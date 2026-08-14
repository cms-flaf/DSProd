# Your first production

DSProd ships a small **test setup** that exercises the whole chain — gridpack generation, the
fused GEN→NanoAOD step, and the merge — on a tiny number of events. It is the fastest way to
confirm your environment works.

## The test setup

`models/X_HH_bbWW/setups/Run3_XHHbbWW_test.yaml` produces a single X→HH→bbWW resonant point
(M-666, which is not a central mass, so no gridpack is stored for it and it is **generated**),
100 events, one era:

```yaml
process: X_HH_bbWW
conditions: config/conditions_Run3.yaml
output: XHHbbWW_test
eras: [ Run3_2022EE ]
nano_versions:
  Run3_2022EE: [ v12, v15 ]
first_step: LHEGS
last_step: NANO
events_per_job: 100
files_per_merge: 10
points:
  - name: GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-666
    mass: 666
    spin: 0
    events_total: 100
```

Set your EOS area once in `config/user_custom.yaml` (`fs.storage_base`); the setup only names the
`output` sub-directory. See [Global & user config](../configuration/settings.md).

## Run it

Run the final task; LAW schedules everything upstream. Start with the merge target on the local
backend:

```bash
source env.sh
law run NanoMergeTask \
  --setup models/X_HH_bbWW/setups/Run3_XHHbbWW_test.yaml \
  --workflow local
```

What happens, in order:

1. **`InstallCMSSW`** builds the CMSSW releases the era needs (first run only; cached afterwards).
2. **`MakeGridpack`** finds no stored M-666 gridpack in DSProdGridpacks, so it generates one from
   the process [cards](../configuration/processes.md).
3. **`RunProd`** runs the fused GEN→…→MiniAOD→NanoAOD chain and stages one nano per requested
   version.
4. **`NanoMergeTask`** merges the per-seed nanos, verifies the event count, and drops the staged
   inputs.

The merged output lands under your storage area (`<fs.storage_base>/XHHbbWW_test`), ready for FLAF.

!!! tip "Run just one stage"
    To stop earlier, run an upstream task directly, e.g. `law run MakeGridpack --setup … --workflow
    local` to only produce the gridpack. Add `--print-status -1` to any command to see what LAW
    considers done vs. pending without running anything.

## Scaling up

The same setup runs on the batch backends by swapping `--workflow`:

```bash
law run RunProd --setup <setup>.yaml --workflow htcondor
law run MakeGridpack --setup <setup>.yaml --workflow crab
```

See [Backends](../concepts/backends.md) for what each one needs.
