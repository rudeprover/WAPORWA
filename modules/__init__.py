# -*- coding: utf-8 -*-
"""
WaPOR v3 Data Collection Module

This module provides functions to download WaPOR v3 data from FAO.
No API token required - data is accessed via public COG files.

Available temporal resolutions:
- Daily (E = Every day)
- Dekadal (D = 10-day periods)
- Monthly (M)
- Annual (A)

Available data levels:
- L1: Continental level (300m resolution)
- L2: National level (100m resolution)

Example usage:
--------------
import WaPOR_v3 as WaPOR

# Download monthly precipitation
WaPOR.PCP_monthly(
    Dir='C:/Temp/', 
    Startdate='2020-01-01', 
    Enddate='2020-12-31',
    latlim=[29, 34], 
    lonlim=[34, 37],
    version=3
)

# Download Level 2 (100m) monthly actual ET
WaPOR.AET_monthly(
    Dir='C:/Temp/', 
    Startdate='2020-01-01', 
    Enddate='2020-12-31',
    latlim=[29, 34], 
    lonlim=[34, 37],
    level=2,
    version=3
)

Authors: Adapted for WaPOR v3
Contact: Based on IHE Delft WaPOR v2 module
Repository: https://github.com/wateraccounting/watools
"""

# Import all main functions from WaPOR_v3
from .WaPOR_v3 import (
    # Precipitation
    PCP_daily,
    PCP_dekadal,
    PCP_monthly,
    PCP_yearly,
    
    # Reference ET
    RET_monthly,
    RET_yearly,
    
    # Actual ET (AETI)
    AET_dekadal,
    AET_monthly,
    AET_yearly,
    
    # Interception
    I_dekadal,
    I_yearly,
    
    # Land Cover Classification
    LCC_yearly,
    
    # Helper functions
    list_available_mapsets,
)

# Import API helpers for advanced usage
from .waporv3_api import (
    BASE_URL,
    get_mapsets,
    get_rasters,
    filter_rasters_by_date,
)

__all__ = [
    # Precipitation
    'PCP_daily',
    'PCP_dekadal', 
    'PCP_monthly',
    'PCP_yearly',
    
    # Reference ET
    'RET_monthly',
    'RET_yearly',
    
    # Actual ET
    'AET_dekadal',
    'AET_monthly',
    'AET_yearly',
    
    # Interception
    'I_dekadal',
    'I_yearly',
    
    # Land Cover
    'LCC_yearly',
    
    # Helpers
    'list_available_mapsets',
    'get_mapsets',
    'get_rasters',
]

__version__ = '3.0.0'
__doc__ = """WaPOR v3 data collection module - no API token required"""

# Print info when module is imported
print("WaPOR v3 module loaded")
print("No API token required - uses public COG files")
print("Use list_available_mapsets() to see all available datasets")