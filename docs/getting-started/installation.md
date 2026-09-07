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

!!! note "Inside a job the delegated proxy is used as-is"
    On a worker node DSProd takes whatever proxy the batch system put in `$X509_USER_PROXY` and
    never renews or removes it: `voms-proxy-init` cannot run unattended there, and CRAB's
    delegated proxy lives for slightly under 24 hours — shorter than the renewal threshold that
    applies interactively.

### The MyProxy credential CRAB needs

The [CRAB backend](../concepts/backends.md) needs a second credential: one delegated to
`myproxy.cern.ch`, which the CRAB TaskWorker retrieves to keep your grid jobs supplied with a proxy
for as long as they run. `voms-proxy-init` does not create it, and it must be valid for **at least
five days** — the TaskWorker refuses anything shorter, so `RunProd --workflow crab` checks it up
front and refuses to submit rather than let a whole production fail hours later.

Renew it with the CRAB client, which asks for your GRID certificate passphrase and stores 30 days:

```bash
crab createmyproxy --days 30
```

!!! note "Run it from a shell with `env.sh` sourced"
    DSProd's `crab` wrapper moves `$HOME` to a scratch directory so CRAB's `~/.crab3` never lands
    on AFS. That directory is node-local and has no `.globus` of its own, so the wrapper symlinks
    your real one into it. Calling the CRAB client from somewhere else works too — it just uses
    your real `$HOME` directly.

!!! warning "`myproxy-init` on its own does not work"
    CRAB looks the credential up under `sha1(<your DN>)` and under no other name, and the
    TaskWorker can only retrieve it if the delegation carries the TaskWorker DNs as its retrieval
    policy. A bare `myproxy-init -d -n -s myproxy.cern.ch` gives neither: it stores the credential
    under the plain DN with no policy, so CRAB never sees it. Only `crab createmyproxy` — which
    asks the CRAB server for the current TaskWorker DNs — produces a usable credential.

    Note also that DSProd submits with `crab submit --proxy <file>`, and passing `--proxy` makes
    the CRAB client skip its own delegation and renewal entirely. Nothing in a production run will
    top this credential up for you.

Check what is currently stored:

```bash
myproxy-info -s myproxy.cern.ch -l "$(voms-proxy-info -identity | tr -d '\n' | sha1sum | cut -d' ' -f1)"
```

#### Delegating from the proxy instead, without the passphrase

`crab createmyproxy` reads the certificate from `$X509_USER_CERT`/`$X509_USER_KEY` when those are
set, and a VOMS proxy file is itself a certificate with an unencrypted key. Pointing them at the
proxy delegates from the proxy and never prompts:

```bash
X509_USER_CERT=$X509_USER_PROXY X509_USER_KEY=$X509_USER_PROXY \
    crab createmyproxy --days 30
```

!!! danger "Set those two variables for that one command only — never export them"
    `$X509_USER_CERT`/`$X509_USER_KEY` sit **ahead of the default proxy** in the GSI credential
    search order, so exporting them changes how every grid client in the shell authenticates, not
    just the delegation. Point them at your certificate and `myproxy-info` — which
    `createmyproxy` runs itself, right after delegating — tries to authenticate with the
    *encrypted* private key, cannot prompt, and fails with
    `unable to get passphrase ... interrupted or cancelled`. CRAB then reports
    `It seems your proxy has not been delegated to myproxy` even though the delegation succeeded.
    Prefix the single command as shown above and the variables die with it.

!!! warning "This does not reduce how often you type the passphrase — it increases it"
    A credential cannot outlive what signed it, and CRAB enforces that by clamping: it reads the
    remaining whole days of whatever `$X509_USER_CERT` points at and delegates for exactly that
    long, ignoring `--days` (`CredentialInteractions.py:172,182` — the 30-day default it compares
    against is an attribute `--days` never touches, so on a proxy the clamp always fires).

    From a `--valid 192:00` proxy the credential is therefore `floor(8 − proxy age)` days, and the
    5-day gate is satisfied **only while the proxy is younger than about 3 days**. Refreshing the
    proxy needs the passphrase as surely as the certificate route does — so this route asks for it
    roughly every 3 days where `crab createmyproxy --days 30` asks every ~25. Once the proxy has a
    day or less left, the command refuses outright with `YOUR USER CERTIFICATE IS EXPIRED`.

Use it as a stop-gap — to top the credential up from a shell that cannot prompt, when you already
have a fresh proxy — and not as the way you keep the credential alive. For that, the certificate
route above is both simpler and far less work.

## CMSSW on demand

You never install CMSSW by hand. Each production step declares the release and `SCRAM_ARCH` it
needs in [`config/conditions_Run3.yaml`](../configuration/conditions.md); `InstallCMSSW` reads
those and builds each release once, under `soft/<CMSSW_VERSION>/`, guarded by a `.installed`
flag so it is idempotent. On HTCondor the shared AFS `soft/` area is visible to the workers; on
CRAB the releases are set up from cvmfs on the worker itself.
