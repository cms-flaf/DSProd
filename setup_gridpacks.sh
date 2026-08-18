#!/usr/bin/env bash

# Set up the DSProdGridpacks store (the `gridpacks/` submodule) for everyday use:
# a sparse checkout that holds only the per-gridpack README.md provenance files, with every
# Git-LFS download disabled. The store then costs well under a megabyte instead of the whole
# gridpack collection, and DSProd materializes a single gridpack on demand when a production
# actually needs it (dsprod/gridpack_store.py).
#
# Safe to re-run. The store is optional: without it DSProd generates every gridpack itself.

set -e

this_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
store="$this_dir/gridpacks"

if ! command -v git-lfs &> /dev/null; then
  echo "git-lfs is not installed; it is needed to fetch gridpacks from the store." >&2
  echo "Install it (or 'git lfs install') and re-run." >&2
  exit 1
fi

if [ ! -e "$store/.git" ]; then
  echo "Initializing the gridpacks submodule (without downloading any gridpack) ..."
  # GIT_LFS_SKIP_SMUDGE keeps the initial checkout from pulling ~30 MB per gridpack
  if ! GIT_LFS_SKIP_SMUDGE=1 git -C "$this_dir" submodule update --init gridpacks; then
    echo
    echo "Could not check out cms-flaf/DSProdGridpacks on gitlab.cern.ch (check your CERN" >&2
    echo "account has access to the cms-flaf group and that your SSH key is registered there)." >&2
    echo "This is not fatal: DSProd generates gridpacks it cannot find there." >&2
    exit 0
  fi
fi

# never fetch an LFS object implicitly; gridpack_store.fetch() asks for the one it needs
git -C "$store" config lfs.fetchexclude '*'
# keep the tarballs out of the working tree, so only the README.md files are checked out
git -C "$store" sparse-checkout set --no-cone '/*' '!*.tar.xz'

echo
echo "Gridpack store ready: $store"
echo "  checked out: README.md provenance files only ($(du -sh "$store" | cut -f1) incl. git metadata)"
echo "  gridpacks:   fetched on demand by the production tasks; committed with CollectGridpacks"
