# Your first production

The fastest way to confirm your environment works is to run a real production setup in **test
mode**: `--test <n>` produces `<n>` events per point and era, and `--eras`/`--points` narrow what
runs. There is no separate test setup to keep in sync, and the nanoAODs go to `<output>_test`, so a
test run cannot overwrite a produced sample.

!!! note "Gridpacks and premix lists are shared with production, on purpose"
    Only the event products carry the `_test` suffix. A gridpack does not depend on the number of
    events, so `ImportGridpack`/`MakeGridpack` read and write the *production* area even under
    `--test`, and a test run reuses a production gridpack instead of regenerating it. The flip side
    is that testing a mass the store does not hold makes a gridpack in the production area.

!!! warning "Finish the setup first"
    This page assumes everything in
    [Before you start a production](installation.md#before-you-start-a-production) is done — the
    submodules, `config/user_custom.yaml`, `source env.sh` in this shell, a Kerberos ticket and a
    VOMS proxy with plenty of time left. A proxy is needed even for `--workflow local`, because
    products are written to grid storage. The MyProxy credential is needed only for
    `--workflow crab`.

## Run it

Run the final task — LAW schedules everything upstream:

```bash
source env.sh
law run NanoMergeTask \
  --setup models/X_HH/setups/Run3_XHHbbWW.yaml \
  --eras Run3_2023BPix \
  --points GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-800 \
  --test 10 \
  --workflow local
```

That is exactly **one** generation job and one merge: one point, one era, ten events. Expect a few
minutes of `cmsRun` across the five steps — plus a one-off cost on a fresh checkout, where
`InstallCMSSW` has to build each release it needs before anything runs. Budget up to an hour for
the very first run, and minutes for every one after it.

!!! danger "Narrow with `--eras` and a full point name, not with `--branches`"
    Both matter more than they look:

    - `--points '*_M-800'` is a **glob over four points** — Radion and BulkGraviton, each in the
      single-lepton (`2B2JLNu`) and dilepton (`2B2L2Nu`) final states — and the setup lists **five
      eras**. That combination is 20 generation jobs, which `--workflow local` runs one after
      another: hours to days, not a smoke test.
    - `--branches 0` does **not** help. It applies only to the task named on the command line, and
      `NanoMergeTask` pulls in the whole `RunProd` workflow deliberately (see
      [Tasks](../concepts/tasks.md)), so all the generation jobs still run and only the merge is
      narrowed.

    Use `--eras` plus a fully-qualified `--points` name, as above, and add `--print-status -1` to
    see what LAW plans without running any of it.

What happens, in order:

1. **`CreateVomsProxy`** checks the proxy — and if under 24 hours are left, deletes it and
   re-creates it, asking for your certificate passphrase.
2. **`InstallCMSSW`** builds the CMSSW releases the era needs (first run only; cached afterwards).
3. **`ImportGridpack`** copies the M-800 gridpack from DSProdGridpacks to your storage area — when
   the store is checked out and holds that mass. Otherwise **`MakeGridpack`** generates one from
   the process [cards](../configuration/processes.md), which takes far longer; pick a non-central
   mass to exercise that deliberately.
4. **`PremixFileList`** resolves the pileup premix files for the era once, via DAS, so the jobs do
   not each query it.
5. **`RunProd`** runs the fused GEN→…→MiniAOD→NanoAOD chain and stages one nano per requested
   version.
6. **`NanoMergeTask`** merges the per-seed nanos, verifies the event count, and drops the staged
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
