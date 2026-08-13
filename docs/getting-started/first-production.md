# Your first production

DSProd ships a small **test setup** that exercises the whole chain — gridpack generation, the
fused GEN→NanoAOD step, and the merge — on a tiny number of events. It is the fastest way to
confirm your environment works.

## The test setup

`config/prod_setups/Run3_XHHbbWW_test.yaml` produces a single X→HH→bbWW resonant point
(M-666, which is not a central mass, so the gridpack is **generated** rather than imported),
100 events, one era:

```yaml
process: X_HH_bbWW
conditions: config/conditions_Run3.yaml
storage: /eos/user/k/kandroso/DSProd/XHHbbWW_test
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

Change `storage:` to your own EOS area before running.

## Run it

Run the final task; LAW schedules everything upstream. Start with the merge target on the local
backend:

```bash
source env.sh
law run NanoMergeTask \
  --setup config/prod_setups/Run3_XHHbbWW_test.yaml \
  --workflow local
```

What happens, in order:

1. **`InstallCMSSW`** builds the CMSSW releases the era needs (first run only; cached afterwards).
2. **`MakeGridpack`** generates the M-666 gridpack from the process
   [cards template](../configuration/processes.md).
3. **`RunProd`** runs the fused GEN→…→MiniAOD→NanoAOD chain and stages one nano per requested
   version.
4. **`NanoMergeTask`** merges the per-seed nanos, verifies the event count, and drops the staged
   inputs.

The merged output lands under your `storage:` path, ready for FLAF.

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
