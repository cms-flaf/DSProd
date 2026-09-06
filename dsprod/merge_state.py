"""Which merge groups can run, decided from storage alone.

`NanoMergeTask` has no `RunProd` requirement: a luigi edge to the generation stage cannot express
"this group's own 50 seeds", because law offers only two shapes and both are wrong here. A
per-seed `RunProd.req(self, branch=n)` is a *branch* task (`branch != -1`, so `is_workflow()` is
False and the remote proxy is bypassed), which luigi would run in-process -- the 7 h chain on the
submitting machine rather than on CRAB. A per-group `req_different_branching(branches=<50 seeds>)`
is a narrowed *workflow*, so every group would carry its own submission: 192 CRAB tasks for one
`Run3_2023BPix` era instead of one. Neither scales, and the first is a trap rather than merely
slow.

So the generation stage is submitted once, as a whole, by `Produce`, and a group's readiness is a
question asked of storage: three directory listings per (era, point, nano version), never a stat
per seed -- an era is thousands of seeds per version, and at one round trip each the stats alone
run for hours.
"""

from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor

MERGED, READY, BLOCKED, BROKEN, UNKNOWN = (
    "merged",
    "ready",
    "blocked",
    "broken",
    "unknown",
)
#: report order: what is done, what can run now, then what cannot
STATES = (MERGED, READY, BLOCKED, BROKEN, UNKNOWN)

#: the three listings a point and nano version needs, as sets of file names; a field is None when
#: that listing could not be read
Listing = namedtuple("Listing", ["merged", "records", "staged"])

#: one classified merge branch. `n_missing`/`n_gone` count seeds, for the detail lines; `unread`
#: names the listings of its point and version that could not be read.
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
        "unread",
    ],
    defaults=((),),
)


def classify_group(version, group, seeds, listing):
    """State of one merge group and the seed counts behind it, from its point's listings.

    A merged group is decided first and on the merged file alone: after a successful merge the
    records are there and the staged files are deliberately gone, which is exactly what `broken`
    looks like otherwise.

    A listing that could not be read therefore never yields `broken` or `ready`: `broken` is the
    state whose remedy is deleting `produced/` records, and one failed listing on a delivered
    point would otherwise advise regenerating seeds that are already merged. A missing record is
    the one thing a merged group cannot have, so `blocked` is still decided without them.
    """
    if listing.merged is not None and f"nano_{version}_{group}.root" in listing.merged:
        return MERGED, 0, 0
    if listing.records is None:
        return UNKNOWN, 0, 0
    n_missing = sum(
        1 for seed in seeds if f"nano_{version}_{seed}.json" not in listing.records
    )
    n_gone = (
        0
        if listing.staged is None
        else sum(
            1 for seed in seeds if f"nano_{version}_{seed}.root" not in listing.staged
        )
    )
    if n_missing:
        return BLOCKED, n_missing, n_gone
    if listing.merged is None or listing.staged is None:
        return UNKNOWN, 0, 0
    if n_gone:
        return BROKEN, n_missing, n_gone
    return READY, 0, 0


def storage_looks_empty(groups):
    """True when storage held nothing at all for any group.

    Worth saying on its own: a production reported as entirely unproduced is more often a wrong
    `output` name or an endpoint that answers "not there" for everything than 4800 seeds nobody
    ran.
    """
    return all(
        g.state == BLOCKED and g.n_missing == g.n_seeds and g.n_gone == g.n_seeds
        for g in groups
    )


def _names(dir_target):
    """File names in a storage directory: an empty set when it is not there, None when the listing
    failed.

    Both answer the same at the gfal layer -- `gfal-ls` exits non-zero either way -- so a listing
    that raises is followed by an existence check, and only a directory that answers "not there"
    is read as empty. The one extra round trip buys the difference between a point nobody has
    produced yet and a delivered point whose listing timed out, which are opposite states with
    opposite remedies.
    """
    try:
        return set(dir_target.listdir())
    except Exception:
        pass
    try:
        return None if dir_target.exists() else set()
    except Exception:
        return None


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

    with ThreadPoolExecutor(max_workers=max(1, threads)) as pool:
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
        listing = listings[(era, pi, version)]
        state, n_missing, n_gone = classify_group(version, group, seeds, listing)
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
                unread=tuple(
                    name for name, v in zip(Listing._fields, listing) if v is None
                ),
            )
        )
    return groups
