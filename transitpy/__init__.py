"""Parse, normalize and extract information from one or multiple GTFS files"""

# Add imports here
# Handle versioneer
from ._version import get_versions
from .datasource.PAN import PAN_Datasource
from .feed import Feed, is_gtfs_path
from .spatial import match_to_grid
from .statistics import (route_stats, stop_stats, transfer_route_stats,
                         transfer_stop_stats)
from .transfers import make_transfers

versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions

from . import _version

__version__ = _version.get_versions()['version']
