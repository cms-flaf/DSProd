# Global & user configuration

Deployment settings — where products are written, and the CRAB compute knobs — are kept **out of
the production setups**, so a setup is backend-agnostic and not tied to one user. They follow the
same pattern as FLAF: a committed `config/global.yaml` with the defaults, merged with a
`config/user_custom.yaml` holding your **user-specific** values.

`config/user_custom.yaml` is layered on top of `config/global.yaml`: scalars and lists override,
nested maps are deep-merged (see `dsprod/config.py`).

!!! important "`user_custom.yaml` is not in the repository"
    It is user-dependent, so it is **git-ignored** and you create it yourself after cloning (see
    the example below). `global.yaml` is committed and holds only settings that are the same for
    everyone.

## Create your `config/user_custom.yaml`

Optional: `global.yaml` already points `fs_default` at the **shared production area at FNAL**, so a
fresh checkout writes there. Create `user_custom.yaml` when you want your own space instead — always
do this for tests, so you do not write into the production tree:

```yaml
# config/user_custom.yaml
fs_default: davs://eoshome-k.cern.ch:8444/eos/user/k/kandroso/DSProd/
```

Replace the host and path with your own EOS area. You can override any `global.yaml` value in
the same file, e.g. the CRAB processing site:

```yaml
fs_default: davs://eoshome-k.cern.ch:8444/eos/user/k/kandroso/DSProd/

crab:
  whitelist: [ T2_CH_CERN ]
  max_cores: 1
```

## `fs_default`

`fs_default` uses the FLAF path notation: **one URI carrying protocol, host and base path**. All
product paths are relative to it, so the storage area lives in exactly one place.

```yaml
fs_default: davs://eoshome-k.cern.ch:8444/eos/user/k/kandroso/DSProd/   # remote (WLCG/gfal)
fs_default: /eos/user/k/kandroso/DSProd/                                # local file system
```

A value starting with `/` selects a local file system; anything else is treated as a remote
(WLCG) one, accessed through the gfal-CLI interface.

!!! note "One file system for every backend"
    `fs_default` is used by **all** backends. A production submitted with `--workflow crab` writes
    exactly where the same setup would write with `--workflow local` or `htcondor` — there is no
    separate CRAB output location. (Later, more granular `fs_*` keys can be added, as in FLAF.)

A production writes to `<fs_default>/<output>`, where `output` is named by the
[production setup](prod-setups.md).

## `config/global.yaml` (committed defaults)

```yaml
crab:
  max_memory_mb: 2500
  max_cores: 1
  # whitelist: [ T2_CH_CERN, ... ]  # optional; empty (default) = all CMS processing sites
  # blacklist: [ ... ]              # optional; exclude misbehaving sites
  # ignore_global_blacklist: true   # optional; waive CMS's known-broken-site list (not recommended)
```

The `crab:` block holds **compute settings only** — CRAB never stages out (DSProd owns all I/O),
so there is no CRAB output location to configure. These can also be overridden per run on the
command line (e.g. `--crab-whitelist`, `--crab-memory`).

**No whitelist by default.** DSProd jobs have no real input dataset, so they can run at any CMS
processing site; leaving the whitelist empty gives the widest pool, and setting one can only narrow
it. See [Backends](../concepts/backends.md#site-selection) for when restricting is worthwhile.

!!! note "On grid workers"
    Being git-ignored does not mean being absent from jobs: the CRAB code tarball ships the whole
    `config/` directory, so your `user_custom.yaml` travels with the job and grid workers resolve
    `fs_default` exactly as your local runs do.

!!! note "law.cfg"
    `config/law.cfg` holds only law/luigi framework settings (scheduler, job dir, modules). Storage
    and CRAB settings are **not** there — they are in `global.yaml` / `user_custom.yaml`.
