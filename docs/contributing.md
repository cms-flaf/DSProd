# Contributing

DSProd follows the same lightweight conventions as the rest of the FLAF ecosystem.

## Branches and pull requests

- Work on a topic branch; never commit directly to `main`.
- Open the pull request against `cms-flaf/DSProd:main`.
- Keep changes surgical and focused.

## Code formatting

Formatting is checked in CI by the **Formatting Check** workflow
(`.github/workflows/formatting-check.yaml`) on every pull request. It runs, on the files changed
in the PR:

- **[black](https://black.readthedocs.io/)** for Python (`.py`);
- **[yamllint](https://yamllint.readthedocs.io/)** for YAML (`.yaml` / `.yml`), using the
  repository's [`.yamllint`](https://github.com/cms-flaf/DSProd/blob/main/.yamllint) config
  (shared with FLAF — note it requires spaces inside flow brackets, e.g. `[ a, b ]`).

Run the same check locally before committing:

```bash
source env.sh          # or: activate an environment with black + yamllint
bash run_tools/apply_format.sh            # reformats Python, checks YAML
bash run_tools/apply_format.sh --dry-run  # check only, no changes
```

`apply_format.sh` looks at the files changed on your branch vs. `origin/main`, so run it after
committing (or with your changes staged) for it to see them.

## Repository sanity

A second workflow, **Check repository state** (`.github/workflows/repo-sanity-checks.yaml`), keeps
the repository small and text-only on every pull request:

- **No binary files** — any changed file detected as binary fails the check; commit large or
  binary assets via [Git LFS](https://git-lfs.com/) instead. (Empty files and directories are not
  treated as binary.)
- **No large size increase** — the PR's effect on the packed repository size is measured against a
  simulated squash merge and must stay under **1024 KiB**. If a large increase is genuinely
  intended, add the **`big changes`** label to the PR to allow it.

## Documentation

These docs are built with [MkDocs](https://www.mkdocs.org/) (Material theme). To preview:

```bash
python3 -m venv /tmp/mkdocs_env
/tmp/mkdocs_env/bin/pip install mkdocs-material
/tmp/mkdocs_env/bin/mkdocs serve   # or: mkdocs build --strict
```

`mkdocs build --strict` fails on broken links and missing nav entries, so run it before opening a
docs PR. When a change alters task names, setup fields, backends, or the conditions format, update
the affected page in the **same** PR.
