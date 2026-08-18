# Installation

DSProd runs on lxplus (or any CERN AlmaLinux9 / cvmfs-enabled node). It needs only a checkout of
the repository and a valid grid proxy; the CMSSW releases it uses are installed on demand.

## Clone the repository

DSProd uses three submodules:

| Submodule | Path | Contents |
|---|---|---|
| `genproductions_scripts` | `genproductions_scripts/` | CMS gridpack generators (GitLab cms-gen) |
| [DSProdModels](https://github.com/cms-flaf/DSProdModels) | `models/` | model plugins + production cards + gen fragments |
| [DSProdGridpacks](https://gitlab.cern.ch/cms-flaf/DSProdGridpacks) | `gridpacks/` | stored gridpacks (CERN GitLab, Git LFS) |

```bash
git clone git@github.com:cms-flaf/DSProd.git
cd DSProd
git submodule update --init models genproductions_scripts
./setup_gridpacks.sh    # optional: the gridpack store (see below)
```

`models` is **required** — it provides the production models; a run fails with a clear error if it
is not checked out.

!!! warning "Do not use `git clone --recursive`"
    A recursive clone would check `gridpacks` out and download every gridpack. Init the submodules
    as above instead, then run `setup_gridpacks.sh`.

    The gridpack store lives on **CERN GitLab** (`gitlab.cern.ch/cms-flaf/DSProdGridpacks`), with
    **internal** visibility: any authenticated CERN account can read it, and no GitHub permission is
    involved. The other two submodules stay on GitHub.

### The gridpack store

`./setup_gridpacks.sh` sets up `gridpacks/` the way it is meant to be used day to day:

- a **sparse checkout** holding only the per-gridpack `README.md` provenance files — a few hundred
  kilobytes instead of the whole gridpack collection;
- **every Git-LFS download disabled** (`lfs.fetchexclude=*`), so no tarball is ever fetched by
  accident.

A gridpack is then fetched **on demand**: `ImportGridpack` streams the one it needs straight from
the LFS server to `fs_default` (see [Tasks](../concepts/tasks.md)). Nothing is written into the
working tree, so the sparse checkout stays intact.

The store is entirely **optional**: without it (or without access to it) DSProd generates every
gridpack itself. The script says so and exits cleanly if the checkout is not permitted.

!!! tip "Adding gridpacks you produced"
    `CollectGridpacks` copies gridpacks this checkout produced back into the store and prints the
    `git add --sparse …` commands to commit them — see [Tasks](../concepts/tasks.md).

## Create your user configuration

`config/user_custom.yaml` holds your personal settings and is **not** part of the repository (it is
git-ignored). It is optional — `config/global.yaml` already points `fs_default` at the shared FNAL
production area — but create it before running anything of your own, so tests do not write into the
production tree:

```yaml
# config/user_custom.yaml
fs_default: davs://eoshome-k.cern.ch:8444/eos/user/k/kandroso/DSProd/
```

Point it at your own EOS area. See [Global & user config](../configuration/settings.md) for the
full set of options and the committed defaults in `config/global.yaml`.

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
