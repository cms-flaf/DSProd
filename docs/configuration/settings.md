# Global & user configuration

Deployment and site settings — the remote storage area and the CRAB backend configuration — are
kept **out of the production setups** so that a setup is backend-agnostic and not tied to one user.
They follow the same pattern as FLAF: a committed `config/global.yaml` with the defaults, merged
with a `config/user_custom.yaml` holding the **user-specific overrides**.

`config/user_custom.yaml` is layered on top of `config/global.yaml`: scalars and lists override,
nested maps are deep-merged (see `dsprod/config.py`).

## `config/global.yaml` (defaults)

```yaml
fs:
  wlcg_base: davs://eoshome-USER.cern.ch:8444/   # law WLCG target base (protocol + host)
  storage_base: /eos/user/U/USERNAME/DSProd      # root EOS dir for all products

crab:
  storage_site: T3_CH_CERNBOX             # Site.storageSite (submit-time write-check only)
  out_lfn_base: /store/user/USERNAME/DSProd_crab  # Data.outLFNDirBase (write-check only)
  whitelist: [ T2_CH_CERN ]               # CMS processing site(s)
  max_memory_mb: 2500
  max_cores: 1
```

## `config/user_custom.yaml` (your overrides)

Edit this for your own EOS area and grid storage:

```yaml
fs:
  wlcg_base: davs://eoshome-k.cern.ch:8444/
  storage_base: /eos/user/k/kandroso/DSProd
crab:
  out_lfn_base: /store/user/kandroso/DSProd_crab
```

## How it is used

- **Storage** — a production writes to `<fs.storage_base>/<output>`, where `output` is named by the
  [production setup](prod-setups.md). `fs.wlcg_base` is the protocol/host the products'
  `/eos/...` paths are served through.
- **CRAB** — the [CRAB backend](../concepts/backends.md) reads the `crab:` block. `storage_site` and
  `out_lfn_base` are only a submit-time write-check (products still go to the storage area above);
  `whitelist` is the CMS processing site. These can also be overridden per-run on the command line
  (e.g. `--crab-whitelist`, `--crab-memory`).

!!! note "law.cfg"
    `config/law.cfg` holds only law/luigi framework settings (scheduler, job dir, modules). Storage
    and CRAB settings are **not** there — they are in `global.yaml` / `user_custom.yaml`.
