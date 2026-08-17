"""Access to the DSProdGridpacks store — the `gridpacks/` submodule.

The store is checked out **sparsely** (see `setup_gridpacks.sh`): the working tree holds only the
per-gridpack `README.md` provenance files, and no Git-LFS object is fetched, so a checkout costs
well under a megabyte instead of the whole gridpack collection.

A gridpack is therefore materialized **on demand**, streamed from the LFS server straight into a
destination path (`git cat-file blob` piped through `git lfs smudge`). Nothing is written into the
working tree, so the sparse checkout stays intact and no gridpack is ever downloaded twice.
"""

import hashlib
import os
import re
import shutil
import subprocess

_LFS_POINTER_PREFIX = b"version https://git-lfs"


def store_root(ana_path=None):
    """Path of the gridpacks store inside a DSProd checkout."""
    return os.path.join(ana_path or os.environ["ANALYSIS_PATH"], "gridpacks")


def is_available(root):
    """Whether the store is checked out at all (it is absent on grid workers)."""
    return os.path.exists(os.path.join(root, ".git"))


def local_path(root, rel):
    """Where a gridpack belongs in the store checkout (used when adding a new one)."""
    return os.path.join(root, rel)


def is_lfs_pointer(path):
    try:
        with open(path, "rb") as f:
            return f.read(len(_LFS_POINTER_PREFIX)) == _LFS_POINTER_PREFIX
    except OSError:
        return False


def _git(root, *args, extra=(), check=True):
    return subprocess.run(
        ["git", "-C", root, *extra, *args],
        capture_output=True,
        check=check,
    )


def contains(root, rel):
    """Whether the store *tracks* `rel` — true even though the sparse checkout omits the file."""
    if not is_available(root):
        return False
    try:
        out = _git(root, "ls-tree", "-r", "HEAD", "--", rel).stdout
    except (subprocess.CalledProcessError, OSError):
        return False
    return bool(out.strip())


def _pointer_info(root, rel):
    """(oid, size) of the LFS object behind `rel`, or None if it is not an LFS pointer."""
    try:
        blob = _git(root, "cat-file", "blob", f"HEAD:{rel}").stdout
    except (subprocess.CalledProcessError, OSError):
        return None
    if not blob.startswith(_LFS_POINTER_PREFIX):
        return None
    text = blob.decode("utf-8", "replace")
    oid = re.search(r"^oid sha256:([0-9a-f]+)$", text, re.M)
    size = re.search(r"^size (\d+)$", text, re.M)
    return (oid.group(1), int(size.group(1))) if oid and size else None


def sha256sum(path, block_size=1 << 22):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(block_size):
            h.update(chunk)
    return h.hexdigest()


def fetch(root, rel, dest):
    """Materialize the stored gridpack `rel` into `dest`. Returns True on success.

    False means "not in the store" (or the download failed) — the caller then generates the
    gridpack instead. The content is verified against the LFS pointer (size + sha256 oid), because
    `git lfs smudge` writes the pointer through unchanged when the object cannot be downloaded.
    """
    if not contains(root, rel):
        return False

    worktree = local_path(root, rel)
    if os.path.isfile(worktree) and not is_lfs_pointer(worktree):
        shutil.copy(worktree, dest)
        return True

    pointer = _pointer_info(root, rel)
    try:
        with open(dest, "wb") as out:
            blob = subprocess.Popen(
                ["git", "-C", root, "cat-file", "blob", f"HEAD:{rel}"],
                stdout=subprocess.PIPE,
            )
            # the store disables LFS downloads globally; ask for this one object explicitly
            smudge = subprocess.Popen(
                ["git", "-C", root, "-c", "lfs.fetchexclude=", "lfs", "smudge"],
                stdin=blob.stdout,
                stdout=out,
            )
            blob.stdout.close()
            rc = smudge.wait()
            blob.wait()
        if rc != 0:
            return False
    except OSError:
        return False

    if pointer is None:  # committed without LFS: the blob itself is the content
        return os.path.isfile(dest)
    oid, size = pointer
    if os.path.getsize(dest) != size or sha256sum(dest) != oid:
        # smudge passes the pointer through when the object cannot be downloaded
        return False
    return True


def git_add_hint(root, rels):
    """Commands that add newly collected gridpacks to the store checkout.

    `--sparse` is required: the sparse checkout excludes `*.tar.xz`, and a plain `git add` skips
    such a path with only a hint.
    """
    paths = " ".join(f"'{os.path.dirname(rel)}'" for rel in sorted(set(rels)))
    return [
        f"git -C {root} add --sparse {paths}",
        f"git -C {root} commit -m 'add <describe the gridpacks>'",
        f"git -C {root} push",
    ]
