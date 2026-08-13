# Backends

Every production task accepts `--workflow <backend>`. The three backends differ only in *where*
the jobs run — the task logic, and all remote EOS I/O, are identical. DSProd always writes its
products to the setup's `storage:` EOS path itself, so the batch systems are used purely for
compute.

| `--workflow` | Runs on | Use for |
|---|---|---|
| `local` | the current machine | tests, small productions, debugging |
| `htcondor` | CERN HTCondor batch | medium productions |
| `crab` | the WLCG grid (CRAB) | large productions needing many sites |

## `local`

Runs branches in the current shell. Combine with `--workers <n>` to run several branches in
parallel. Needs a valid VOMS proxy for the EOS writes. This is the backend used by the
[first-production walkthrough](../getting-started/first-production.md).

## `htcondor`

Submits to the CERN HTCondor pool. Jobs bootstrap from the shared AFS checkout (`bootstrap.sh`)
and see the installed CMSSW releases under `soft/` directly. Relevant knobs (all `significant=False`,
so they do not change task identity):

- `--max-runtime <hours>` (per-task defaults: MakeGridpack 12 h, RunProd 24 h, NanoMergeTask 3 h);
- `--n-cpus <n>` (RunProd defaults to 4);
- `--krenew <hours>` — how often to renew the Kerberos ticket while polling.

Jobs request AlmaLinux9 workers and write their HTCondor logs under `data/logs/`.

## `crab`

Submits to the WLCG grid via [CRAB](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideCrab),
built on `law.contrib.cms.CrabWorkflow`. This is the backend for large-scale private production,
where CERN HTCondor alone does not provide enough resources.

Because WLCG workers have **no AFS**, the DSProd code (plus `genproductions_scripts` and the
vendored `law`/`luigi`) is shipped as a CRAB `inputFiles` tarball, built at submit time and
unpacked by `bootstrap.sh`; CMSSW is set up from cvmfs on the worker. DSProd owns all output and
log I/O (products go to the `storage:` EOS path via the gfal-CLI interface), so CRAB's own
stageout and log transfer are forced off.

### Requirements

- a VOMS proxy **and** a MyProxy credential valid for at least 5 days (see
  [Installation](../getting-started/installation.md));
- a `crab:` block in the [production setup](../configuration/prod-setups.md):

```yaml
crab:
  storage_site: T3_CH_CERNBOX             # Site.storageSite (submit-time write-check only)
  out_lfn_base: /store/user/<you>/DSProd  # Data.outLFNDirBase (write-check only)
  whitelist: [ T2_CH_CERN ]               # a real CMS *processing* site
  max_memory_mb: 2500
  max_cores: 1
```

!!! warning "storage_site vs. whitelist"
    `storage_site`/`out_lfn_base` are only a submit-time write-check — the real products go to the
    `storage:` EOS path. `whitelist` must be a genuine CMS **processing** site: do **not** put the
    storage site (e.g. `T3_CH_CERNBOX`) there, or CRAB refuses the submission. Generation jobs have
    no input dataset, so any processing site works; `T2_CH_CERN` keeps them near CERNBOX.

### Debugging CRAB jobs

`crab status`/`crab getlog` re-delegate a MyProxy interactively. To inspect a job without that,
fetch its stdout directly from the task's web directory with your VOMS proxy — remember
`--capath /etc/grid-security/certificates`, or curl returns HTTP 000. The
[CRAB backend module](https://github.com/cms-flaf/DSProd/blob/main/dsprod/crab.py) documents the
details.
