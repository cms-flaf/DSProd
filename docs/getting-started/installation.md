# Installation

DSProd runs on lxplus (or any CERN AlmaLinux9 / cvmfs-enabled node). It needs only a checkout of
the repository and a valid grid proxy; the CMSSW releases it uses are installed on demand.

## Clone the repository

DSProd uses one submodule, `genproductions_scripts` (the CMS gridpack generators), so clone
recursively:

```bash
git clone --recursive ssh://git@github.com:cms-flaf/DSProd.git
cd DSProd
```

If you already cloned without `--recursive`:

```bash
git submodule update --init --recursive
```

## Source the environment

```bash
source env.sh
```

Sourcing `env.sh` sets up LAW and a Python/ROOT stack. It does **not** build CMSSW — the
per-era releases are installed later, on demand, by the [`InstallCMSSW`](../concepts/tasks.md)
task. The first time you source it on lxplus, `env.sh` also vendors a pure-python copy of
`law`/`luigi` into `soft/vendor` (used by the [CRAB backend](../concepts/backends.md) on grid
workers).

After sourcing you can list the available tasks:

```bash
law index
```

## Grid proxy

Every backend needs a valid VOMS proxy (products are written to EOS). Create one before running:

```bash
voms-proxy-init --rfc --voms cms --valid 192:00
```

DSProd looks for the proxy at `$X509_USER_PROXY` (falling back to `data/voms.proxy`).

!!! tip "CRAB needs a MyProxy credential too"
    The [CRAB backend](../concepts/backends.md) additionally needs a MyProxy credential valid for
    at least five days. `voms-proxy-init` does not create it; CRAB does so on first submission,
    or you can refresh it with `myproxy-init`.

## CMSSW on demand

You never install CMSSW by hand. Each production step declares the release and `SCRAM_ARCH` it
needs in [`config/conditions_Run3.yaml`](../configuration/conditions.md); `InstallCMSSW` reads
those and builds each release once, under `soft/<CMSSW_VERSION>/`, guarded by a `.installed`
flag so it is idempotent. On HTCondor the shared AFS `soft/` area is visible to the workers; on
CRAB the releases are set up from cvmfs on the worker itself.
