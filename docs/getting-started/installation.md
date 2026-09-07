# Installation

DSProd runs on lxplus (or any CERN AlmaLinux9 / cvmfs-enabled node). The CMSSW releases it uses
are installed on demand, so there is nothing to build by hand — but a few things have to be in
place on the account before any of the steps below work.

## What you need first

| Prerequisite | Why | Check |
|---|---|---|
| A cvmfs- and AFS-enabled CERN node | CMSSW, the CRAB client and the gridpack tooling all come from `/cvmfs/cms.cern.ch` | `ls /cvmfs/cms.cern.ch` |
| Membership of the **CMS VO** | every backend writes to grid storage as a CMS user | [voms-admin](https://voms2.cern.ch:8443/voms/cms) |
| A **grid certificate** installed as `~/.globus/usercert.pem` + `~/.globus/userkey.pem` | both credential steps below read it; nothing creates it for you | `openssl x509 -noout -subject -enddate -in ~/.globus/usercert.pem` |
| An **SSH key registered on GitHub** *and* one on **CERN GitLab** | the submodules are cloned over SSH from both hosts | `ssh -T git@github.com` · `ssh -T git@gitlab.cern.ch` |
| A **Kerberos ticket** (and AFS token) | DSProd renews these while a production polls, but it can only ever renew — it never runs `kinit` | `klist` · `tokens` |

!!! warning "DSProd renews Kerberos, it does not create it"
    `kinit -R` fails outright on an empty credential cache, so start a production from a shell
    with a live ticket (`kinit`) and re-run `kinit` yourself before the ticket's *renewable*
    lifetime runs out. A production that outlives it loses AFS mid-run.

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

`models` is **required** — it provides the production models. Forgetting it does not produce a
friendly message: the run ends in a `FileNotFoundError` traceback naming the setup yaml
(`models/X_HH/setups/…`), which means this step was skipped.

!!! warning "Do not use `git clone --recursive`"
    A recursive clone would check `gridpacks` out and download every gridpack. Init the submodules
    as above instead, then run `setup_gridpacks.sh`.

    The gridpack store lives on **CERN GitLab** (`gitlab.cern.ch/cms-flaf/DSProdGridpacks`), with
    **internal** visibility: any authenticated CERN account can read it, and no GitHub permission is
    involved.

    All three submodule remotes are SSH URLs, and they are split across two hosts: `models` is on
    GitHub, while `genproductions_scripts` and `gridpacks` are both on CERN GitLab. You therefore
    need a key registered on each — a missing GitLab key shows up only as
    `Permission denied (publickey)` on `genproductions_scripts`.

### The gridpack store

`./setup_gridpacks.sh` sets up `gridpacks/` the way it is meant to be used day to day:

- a **sparse checkout** holding only the per-gridpack `README.md` provenance files — a few hundred
  kilobytes instead of the whole gridpack collection;
- **every Git-LFS download disabled** (`lfs.fetchexclude=*`), so no tarball is ever fetched by
  accident.

A gridpack is then fetched **on demand**: `ImportGridpack` streams the one it needs straight from
the LFS server to `fs_default` (see [Tasks](../concepts/tasks.md)). Nothing is written into the
working tree, so the sparse checkout stays intact.

The store is **optional in the sense that nothing breaks without it**: DSProd generates any
gridpack it cannot find, and the script exits cleanly if the checkout is not permitted. It is
still worth running — generating a gridpack takes tens of minutes to hours, where importing a
stored one takes seconds, and for the walkthrough in
[Your first production](first-production.md) the stored gridpacks are what make the run short.

The script needs `git-lfs` and SSH access to `gitlab.cern.ch`.

!!! tip "Adding gridpacks you produced"
    `CollectGridpacks` copies gridpacks this checkout produced back into the store and prints the
    `git add --sparse …` commands to commit them — see [Tasks](../concepts/tasks.md).

## Create your user configuration

`config/user_custom.yaml` holds your personal settings and is **not** part of the repository (it is
git-ignored). Create it **before running anything**: `config/global.yaml` points `fs_default` at the
*shared* FNAL production area, so a checkout without a `user_custom.yaml` writes its output into the
production tree rather than somewhere of your own.

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

Source it **in every new shell** — it is not a one-off install step. Without it there is no `law`
on `PATH` and no `ANALYSIS_PATH`, which surfaces as `task 'NanoMergeTask' not found` or a bare
`KeyError: 'ANALYSIS_PATH'`, neither of which mentions `env.sh`.

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
source env.sh          # sets X509_USER_PROXY to data/voms.proxy if it is not already set
voms-proxy-init --rfc --voms cms --valid 192:00 --out "$X509_USER_PROXY"
```

Verify it:

```bash
voms-proxy-info -file "$X509_USER_PROXY" -timeleft   # seconds remaining
```

The `--out` matters. DSProd reads the proxy from `$X509_USER_PROXY`, which `env.sh` sets to
`data/voms.proxy` **only when the variable is not already set**; a bare `voms-proxy-init` writes to
the client's own default instead. Passing `--out "$X509_USER_PROXY"` after sourcing `env.sh` puts it
where DSProd will look, whatever the shell already had.

!!! warning "A run will renew the proxy itself — interactively"
    `RunProd` requires `CreateVomsProxy`, which treats a proxy with less than **24 hours** left as
    unusable: it then **deletes** it and runs `voms-proxy-init … --valid 192:00`, which stops to ask
    for your certificate passphrase. Start long productions with a fresh proxy so this does not
    happen hours into a run. (`--CreateVomsProxy-time-limit` changes the threshold.)

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
myproxy-info -s myproxy.cern.ch \
    -l "$(voms-proxy-info -identity | tr -d '\n' | sha1sum | cut -d' ' -f1)"
```

`timeleft` must read at least 5 days for `--workflow crab` to submit.

!!! note "Create the VOMS proxy first"
    The name is derived from the proxy, so with no valid proxy `voms-proxy-info -identity` prints
    nothing, the command asks about the sha1 of the empty string, and `myproxy-info` then falls
    back to certificate authentication and prompts for your passphrase — for what should be a
    read-only check.

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

## Before you start a production

Everything above, in order, and how often each part has to be redone.

### Once per checkout

```bash
git clone git@github.com:cms-flaf/DSProd.git && cd DSProd
git submodule update --init models genproductions_scripts
./setup_gridpacks.sh
printf 'fs_default: davs://eoshome-X.cern.ch:8444/eos/user/X/YOU/DSProd/\n' > config/user_custom.yaml
```

### Once every ~25 days — the MyProxy credential

Only needed for `--workflow crab`. Asks for your certificate passphrase.

```bash
crab createmyproxy --days 30
```

### Once every few days — the VOMS proxy

Asks for your certificate passphrase. Give it a comfortable margin: a proxy under 24 hours makes a
run stop and re-create it interactively.

```bash
voms-proxy-init --rfc --voms cms --valid 192:00 --out "$X509_USER_PROXY"
```

### Every new shell

```bash
source env.sh
kinit                  # if `klist` shows no live ticket
```

### Then check all of it at once

```bash
source env.sh
klist        >/dev/null 2>&1 && echo "kerberos   OK" || echo "kerberos   MISSING -- run kinit"
voms-proxy-info -file "$X509_USER_PROXY" -exists -valid 24:00 \
             && echo "voms proxy OK" || echo "voms proxy TOO SHORT -- see 'Grid proxy' above"
test -f config/user_custom.yaml \
             && echo "user cfg   OK" || echo "user cfg   MISSING -- output would go to the shared area"
test -f models/X_HH/setups/Run3_XHHbbWW.yaml \
             && echo "models     OK" || echo "models     MISSING -- run the submodule update"
law index >/dev/null 2>&1 \
             && echo "law        OK" || echo "law        NOT SET UP -- did env.sh run?"
# crab only, and only for --workflow crab:
myproxy-info -s myproxy.cern.ch \
    -l "$(voms-proxy-info -identity | tr -d '\n' | sha1sum | cut -d' ' -f1)" 2>/dev/null \
    | sed -n 's/^  timeleft:/myproxy   /p'
```

Every line must print `OK`, and the `myproxy` line must show at least 5 days if you intend to use
CRAB. With that done, go to [Your first production](first-production.md).

!!! tip "`crab` works before the first run, too"
    The CRAB client refuses to start without a CMSSW environment, and a fresh checkout has no
    release yet — they are installed during the first run. DSProd's `crab` wrapper therefore
    cmsenv's a read-only release straight from cvmfs when `soft/` is still empty, so
    `crab createmyproxy` works from the moment `env.sh` has been sourced.
