#!/usr/bin/env python3
"""Report which `NanoMergeTask` groups are ready to merge, blocked, or already merged.

`NanoMergeTask` requires only the seeds of the groups it is asked to merge, so a group whose own
seeds are on storage can be merged while the rest of the production is still generating. This
prints the `--branches` value that selects exactly those groups, which is what makes

    law run NanoMergeTask --setup <setup> --branches <ready>

a procedure rather than folklore. In the Run3_2023BPix production 169 of 192 groups were complete
and none had merged.

A group is reported as

  * `merged`  -- its merged file is on storage; nothing left to do;
  * `ready`   -- every seed has a `produced/` record and its staged nano file;
  * `blocked` -- at least one seed has no record yet, i.e. `RunProd` still owes it;
  * `broken`  -- every seed is recorded but a staged file is gone and the group is not merged.
                 `NanoMergeTask` refuses such a group (its merged file was removed after the
                 fact); delete the affected seeds' records so they are produced again.

Storage is read with three directory listings per (era, point, nano version) -- the merged files,
the records and the staged nanos -- never a stat per seed: a full era is thousands of seeds per
version, and at one round trip each the stats alone run for hours.

Run it from a production area with the environment set up:

    source env.sh
    run_tools/merge_status.py --setup models/X_HH/setups/Run3_XHHbbWW.yaml --eras Run3_2023BPix

Exit codes: 0 = report written, 1 = at least one group is broken.
"""

import argparse
import os
import sys
from collections import Counter, namedtuple
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from law.util import range_join

REPO = Path(__file__).resolve().parent.parent

MERGED, READY, BLOCKED, BROKEN = "merged", "ready", "blocked", "broken"
#: report order: what is done, what can run now, then what cannot
STATES = (MERGED, READY, BLOCKED, BROKEN)

#: how many blocked/broken groups are listed individually before the tail is summarised
DETAIL_LIMIT = 20

#: the three listings a point and nano version needs, as sets of file names
Listing = namedtuple("Listing", ["merged", "records", "staged"])

#: one classified merge branch. `n_missing`/`n_gone` count seeds, for the detail lines.
Group = namedtuple(
    "Group",
    [
        "branch",
        "era",
        "point",
        "version",
        "group",
        "n_seeds",
        "state",
        "n_missing",
        "n_gone",
    ],
)


def classify_group(version, group, seeds, listing):
    """State of one merge group and the seed counts behind it, from its point's listings.

    A merged group is decided first and on the merged file alone: after a successful merge the
    records are there and the staged files are deliberately gone, which is exactly what `broken`
    looks like otherwise.
    """
    if f"nano_{version}_{group}.root" in listing.merged:
        return MERGED, 0, 0
    n_missing = sum(
        1 for seed in seeds if f"nano_{version}_{seed}.json" not in listing.records
    )
    n_gone = sum(
        1 for seed in seeds if f"nano_{version}_{seed}.root" not in listing.staged
    )
    if n_missing:
        return BLOCKED, n_missing, n_gone
    if n_gone:
        return BROKEN, n_missing, n_gone
    return READY, 0, 0


def storage_looks_empty(groups):
    """True when storage held nothing at all for any group.

    The gfal interface answers a failed listing and a missing directory the same way, so a
    production this reports as entirely unproduced may instead be one whose endpoint cannot be
    reached -- worth saying, because the two need very different actions.
    """
    return all(
        g.state == BLOCKED and g.n_missing == g.n_seeds and g.n_gone == g.n_seeds
        for g in groups
    )


def _names(dir_target):
    """File names in a storage directory, empty when it does not exist (or cannot be read)."""
    try:
        return set(dir_target.listdir())
    except Exception:
        return set()


def read_listings(task, keys, threads=16):
    """The three listings of each (era, point index, version) in `keys`, read in parallel."""

    def read(key):
        era, pi, version = key
        point = task.prod_points[pi]
        # the group and seed here only name *a* file in each directory, whose parent is what is
        # listed: every group and seed of a point and version share these three directories
        return key, Listing(
            merged=_names(task.merged_nano_target(era, point, version, 0).parent),
            records=_names(task.produced_nano_target(era, point, version, 1).parent),
            staged=_names(task.staged_nano_target(era, point, version, 1).parent),
        )

    with ThreadPoolExecutor(max_workers=threads) as pool:
        return dict(pool.map(read, keys))


def classify(task, threads=16):
    """Classify every branch of `task`'s effective branch map against storage."""
    branch_map = task.get_branch_map()
    keys = sorted(
        {(era, pi, version) for era, pi, version, _, _ in branch_map.values()}
    )
    listings = read_listings(task, keys, threads=threads)
    groups = []
    for branch, (era, pi, version, group, seeds) in sorted(branch_map.items()):
        state, n_missing, n_gone = classify_group(
            version, group, seeds, listings[(era, pi, version)]
        )
        groups.append(
            Group(
                branch=branch,
                era=era,
                point=task.process.point_name(task.prod_points[pi]),
                version=version,
                group=group,
                n_seeds=len(seeds),
                state=state,
                n_missing=n_missing,
                n_gone=n_gone,
            )
        )
    return groups


def _n(count, noun):
    """`3 groups` / `1 group` -- a report that says "1 groups" reads as a broken script."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def detail_line(g):
    """One line about a group that cannot be merged, saying what it is waiting for."""
    where = f"branch {g.branch:<6} {g.era}/{g.point}/{g.version} group {g.group}"
    if g.state == BLOCKED:
        return f"{where}: {g.n_missing} of {g.n_seeds} seeds have no produced record"
    return (
        f"{where}: all {g.n_seeds} seeds recorded but {_n(g.n_gone, 'staged file')} gone "
        "(delete those seeds' records to produce them again)"
    )


def merge_command(args, groups):
    """The `law run` line that merges exactly the ready groups, or None when none are."""
    ready = [g.branch for g in groups if g.state == READY]
    if not ready:
        return None
    parts = ["law run NanoMergeTask", f"--setup {args.setup}"]
    if args.eras:
        parts.append(f"--eras '{args.eras}'")
    if args.points:
        parts.append(f"--points '{args.points}'")
    if args.test:
        parts.append(f"--test {args.test}")
    parts.append(f"--branches {range_join(sorted(ready), to_str=True)}")
    return " ".join(parts)


def report(args, groups, out=sys.stdout):
    """Print the report and return the process exit code."""
    per_era = {}
    for g in groups:
        per_era.setdefault(g.era, Counter())[g.state] += 1
    total = Counter(g.state for g in groups)

    print(f"{len(groups)} merge groups of {args.setup}", file=out)
    for era in sorted(per_era):
        counts = per_era[era]
        line = "  ".join(f"{state} {counts[state]}" for state in STATES)
        print(f"  {era:16} {line}", file=out)
    if len(per_era) > 1:
        line = "  ".join(f"{state} {total[state]}" for state in STATES)
        print(f"  {'all eras':16} {line}", file=out)

    if storage_looks_empty(groups):
        print(
            "\nnothing of this production is on fs_default. If it has already produced files, "
            "the storage endpoint or the VOMS proxy is the problem, not the production: a "
            "listing that fails is indistinguishable from a directory that is not there.",
            file=out,
        )

    unmergeable = [g for g in groups if g.state in (BLOCKED, BROKEN)]
    if unmergeable:
        shown = unmergeable if args.all else unmergeable[:DETAIL_LIMIT]
        print(f"\nnot mergeable ({_n(len(unmergeable), 'group')}):", file=out)
        for g in shown:
            print(f"  {detail_line(g)}", file=out)
        if len(shown) < len(unmergeable):
            print(
                f"  ... and {len(unmergeable) - len(shown)} more (--all lists them)",
                file=out,
            )

    command = merge_command(args, groups)
    if command is None:
        print("\nnothing is ready to merge", file=out)
    else:
        print(
            f"\nready to merge now ({_n(total[READY], 'group')}):\n\n  {command}\n",
            file=out,
        )

    return 1 if total[BROKEN] else 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        epilog="Every group's state comes from storage alone, so this is safe to run while a "
        "production is being driven.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--setup", required=True, help="path to the production setup YAML"
    )
    parser.add_argument(
        "--eras",
        default="",
        help="only these eras (comma-separated fnmatch globs, as in law)",
    )
    parser.add_argument(
        "--points",
        default="",
        help="only these points (comma-separated fnmatch globs)",
    )
    parser.add_argument(
        "--test",
        type=int,
        default=0,
        help="report the `--test <n>` area instead of the production",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=16,
        help="directory listings to run at once; latency-bound, raise it on a slow endpoint "
        "(default: %(default)s)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help=f"list every group that cannot be merged, not just the first {DETAIL_LIMIT}",
    )
    args = parser.parse_args(argv)

    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    os.environ.setdefault("ANALYSIS_PATH", str(REPO))
    os.environ.setdefault("ANALYSIS_DATA_PATH", os.path.join(str(REPO), "data"))
    from dsprod.tasks import NanoMergeTask

    task = NanoMergeTask(
        setup=args.setup,
        eras=tuple(e for e in args.eras.split(",") if e),
        points=tuple(p for p in args.points.split(",") if p),
        test=args.test,
        workflow="local",
    )
    return report(args, classify(task, threads=args.threads))


if __name__ == "__main__":
    sys.exit(main())
