# -*- coding: utf-8 -*-

"""Top-level package for pydsm."""

__author__ = """Nicky Sandhu"""
__email__ = "psandhu@water.ca.gov"
__all__ = [
    "__version__",
    # GTM helper exports
    "build_timewindow_for_time",
    "get_interpolated_cell_concentrations",
    "write_gtm_restart",
]

from ._version import __version__
from .gtmh5 import build_timewindow_for_time, get_interpolated_cell_concentrations
from .create_gtm_restart import write_gtm_restart
