"""WLCG remote file system and targets, vendored from FLAF/RunKit/law_wlcg.py.

Backed by the gfal-CLI :class:`GFALFileInterface` (works on CRAB/WLCG workers). FLAF's
Rucio branch is dropped: DSProd only ever writes to / reads from explicit protocol URLs
(davs://, root://, ...), never a site-prefixed Rucio dataset.
"""

__all__ = ["WLCGFileSystem", "WLCGFileTarget", "WLCGDirectoryTarget"]

from law.target.remote import (
    RemoteFileSystem,
    RemoteTarget,
    RemoteFileTarget,
    RemoteDirectoryTarget,
)

from .law_gfal import GFALFileInterface
from .grid_tools import path_to_pfn


class WLCGFileSystem(RemoteFileSystem):
    def __init__(self, base, local_path_cache_validity_period=600, verbose=0):
        if isinstance(base, str):
            base = [base]
        base_pfns = [path_to_pfn(b) for b in base]
        file_interface = GFALFileInterface(
            base_pfns,
            local_path_cache_validity_period=local_path_cache_validity_period,
            verbose=verbose,
        )
        super(WLCGFileSystem, self).__init__(file_interface)


class WLCGTarget(RemoteTarget):
    def __init__(self, path, fs, **kwargs):
        RemoteTarget.__init__(self, path, fs, **kwargs)


class WLCGFileTarget(WLCGTarget, RemoteFileTarget):
    pass


class WLCGDirectoryTarget(WLCGTarget, RemoteDirectoryTarget):
    pass
