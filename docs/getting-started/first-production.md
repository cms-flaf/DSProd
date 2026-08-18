# Your first production

The fastest way to confirm your environment works is to run a real production setup in **test
mode**: `--test <n>` produces `<n>` events per point and era in a single job, and `--points`
narrows it to one sample. There is no separate test setup to keep in sync — and because test
products go to `<output>_test`, they can never overwrite a production sample.

## Run it

Set your EOS area once in `config/user_custom.yaml` (`fs_default`); the setup only names the
`output` sub-directory. See [Global & user config](../configuration/settings.md).

Then run the final task — LAW schedules everything upstream:

```bash
source env.sh
law run NanoMergeTask \
  --setup models/X_HH/setups/Run3_XHHbbWW.yaml \
  --points '*_M-800' --test 100 \
  --workflow local
```

This produces 100 events of the M-800 single-lepton sample in each era of the setup. Add
`--branches 0` (or a narrower `--points`) to keep it to a single job while you are just checking
the environment.

What happens, in order:

1. **`InstallCMSSW`** builds the CMSSW releases the era needs (first run only; cached afterwards).
2. **`ImportGridpack`** copies the M-800 gridpack from DSProdGridpacks to your storage area. (For a
   mass that is not stored there, **`MakeGridpack`** generates one from the process
   [cards](../configuration/processes.md) instead — pick a non-central mass to exercise that.)
3. **`RunProd`** runs the fused GEN→…→MiniAOD→NanoAOD chain and stages one nano per requested
   version.
4. **`NanoMergeTask`** merges the per-seed nanos, verifies the event count, and drops the staged
   inputs.

The merged output lands under your storage area (`<fs_default>/XHHbbWW_test`, the `_test` suffix
coming from `--test`), ready for FLAF.

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
