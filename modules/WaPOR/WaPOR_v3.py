# -*- coding: utf-8 -*-
"""
WaPOR v3 Data Downloader
Mimics the WaPOR v2 interface but uses WaPOR v3 API (no authentication needed)
Compatible with existing WAPORWA workflows
"""

import os
import re
from osgeo import gdal
import numpy as np
from datetime import datetime

# Import the API helper (make sure waporv3_api.py is in same directory or in PYTHONPATH)
try:
    from waporv3_api import BASE_URL, get_rasters, filter_rasters_by_date
except ImportError:
    print("ERROR: Cannot import waporv3_api. Make sure waporv3_api.py is in the same directory.")
    raise

# Import GIS functions (from your existing code)
try:
    import GIS_functions as gis
except ImportError:
    print("WARNING: GIS_functions not found. Make sure it's in your PYTHONPATH.")
    gis = None


# Scale factors for WaPOR v3 datasets
# Most precipitation and ET products use 0.1 scale factor
SCALE_FACTORS = {
    # Precipitation (mm)
    "L1-PCP-E": 0.1,
    "L1-PCP-D": 0.1,
    "L1-PCP-M": 0.1,
    "L1-PCP-A": 0.1,
    
    # Reference ET (mm)
    "L1-RET-E": 0.1,
    "L1-RET-D": 0.1,
    "L1-RET-M": 0.1,
    "L1-RET-A": 0.1,
    
    # Actual ET and Interception (mm)
    "L1-AETI-D": 0.1,
    "L1-AETI-M": 0.1,
    "L1-AETI-A": 0.1,
    "L2-AETI-D": 0.1,
    "L2-AETI-M": 0.1,
    "L2-AETI-A": 0.1,
    "L1-I-D": 0.1,
    "L1-I-M": 0.1,
    "L1-I-A": 0.1,
    "L2-I-D": 0.1,
    "L2-I-M": 0.1,
    "L2-I-A": 0.1,
    
    # Transpiration (mm)
    "L1-T-D": 0.1,
    "L1-T-M": 0.1,
    "L1-T-A": 0.1,
    "L2-T-D": 0.1,
    "L2-T-M": 0.1,
    "L2-T-A": 0.1,
    
    # Net Primary Production (gC/m2)
    "L1-NPP-D": 1.0,
    "L1-NPP-M": 1.0,
    "L1-NPP-A": 1.0,
    "L2-NPP-D": 1.0,
    "L2-NPP-M": 1.0,
    "L2-NPP-A": 1.0,
    
    # Land Cover (categorical - no scaling)
    "L1-LCC-A": 1.0,
    "L2-LCC-A": 1.0,
}


def _download_mapset(mapset_code, Dir, latlim, lonlim, 
                     Startdate='', Enddate='', Waitbar=1):
    """
    Generic WaPOR v3 downloader.
    
    Parameters
    ----------
    mapset_code : str
        WaPOR v3 mapset code (e.g., 'L1-PCP-E', 'L2-AETI-M')
    Dir : str
        Output directory
    latlim : list
        [ymin, ymax] latitude limits
    lonlim : list
        [xmin, xmax] longitude limits
    Startdate : str
        Start date 'YYYY-MM-DD'
    Enddate : str
        End date 'YYYY-MM-DD'
    Waitbar : int
        Show progress (1) or not (0)
    """
    
    if Waitbar:
        print(f"\nDownloading WaPOR v3 mapset: {mapset_code}")
        print(f"Date range: {Startdate} to {Enddate}")
        print(f"Bounding box: lat {latlim}, lon {lonlim}")
    
    # Create output directory
    out_dir = os.path.join(Dir, mapset_code)
    os.makedirs(out_dir, exist_ok=True)
    
    # Define bounding box for GDAL (xmin, ymin, xmax, ymax)
    bbox = [lonlim[0], latlim[0], lonlim[1], latlim[1]]
    
    # Get all rasters for this mapset
    try:
        all_rasters = get_rasters(mapset_code, include_url=True)
    except Exception as e:
        print(f"ERROR: Could not get rasters for {mapset_code}: {e}")
        return
    
    if not all_rasters:
        print(f"WARNING: No rasters found for {mapset_code}")
        return
    
    # Filter by date if specified
    if Startdate and Enddate:
        all_rasters = filter_rasters_by_date(all_rasters, Startdate, Enddate)
        if not all_rasters:
            print(f"WARNING: No rasters found in date range {Startdate} to {Enddate}")
            return
    
    n_total = len(all_rasters)
    scale = SCALE_FACTORS.get(mapset_code, 1.0)
    
    if Waitbar:
        print(f"Found {n_total} rasters to download")
        if scale != 1.0:
            print(f"Using scale factor: {scale}")
    
    # Download and process each raster
    for i, (code, url) in enumerate(all_rasters, start=1):
        # Create filename from raster code
        # Remove mapset prefix to get date part
        date_part = code.split('.')[-1] if '.' in code else code
        fname = f"{mapset_code}.{date_part}.tif"
        out_path = os.path.join(out_dir, fname)
        
        # Skip if already exists
        if os.path.exists(out_path):
            if Waitbar:
                print(f"[{i}/{n_total}] Skipping existing: {fname}")
            continue
        
        if Waitbar:
            print(f"[{i}/{n_total}] Downloading: {fname}")
        
        try:
            # Download and clip using GDAL
            # Use /vsicurl/ to read directly from URL
            warp_options = gdal.WarpOptions(
                outputBounds=bbox,  # xmin, ymin, xmax, ymax
                dstNodata=-9999,
                creationOptions=['COMPRESS=LZW', 'TILED=YES']
            )
            
            ds = gdal.Warp(out_path, f"/vsicurl/{url}", options=warp_options)
            
            if ds is None:
                print(f"ERROR: Failed to download {fname}")
                continue
            
            ds = None  # Close dataset
            
            # Apply scaling and clean data if GIS_functions available
            if gis is not None:
                try:
                    driver, NDV, xsize, ysize, GeoT, Projection = gis.GetGeoInfo(out_path)
                    Array = gis.OpenAsArray(out_path, nan_values=True)
                    
                    # Apply WaPOR conventions:
                    # - Negative values -> 0 (except NoData)
                    # - Apply scale factor
                    Array = np.where(np.isnan(Array), NDV, Array)
                    Array = np.where(Array < 0, 0, Array)
                    
                    if scale not in (1, 1.0, None):
                        Array = Array * scale
                    
                    gis.CreateGeoTiff(out_path, Array.astype('float32'),
                                    driver, NDV, xsize, ysize, GeoT, Projection)
                except Exception as e:
                    print(f"WARNING: Could not apply scaling to {fname}: {e}")
            
        except Exception as e:
            print(f"ERROR downloading {fname}: {e}")
            if os.path.exists(out_path):
                os.remove(out_path)
            continue
    
    if Waitbar:
        print(f"Finished downloading {mapset_code}")


# =============================================================================
# Public functions matching WaPOR v2 interface
# =============================================================================

def PCP_daily(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
              latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
              version=3, Waitbar=1):
    """
    Download WaPOR v3 daily precipitation (L1-PCP-E).
    
    Parameters
    ----------
    Dir : str
        Output directory
    Startdate : str
        Start date 'YYYY-MM-DD'
    Enddate : str
        End date 'YYYY-MM-DD'
    latlim : list
        [ymin, ymax] latitude limits
    lonlim : list
        [xmin, xmax] longitude limits
    version : int
        WaPOR version (only 3 supported)
    Waitbar : int
        Show progress (1) or not (0)
    """
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    _download_mapset("L1-PCP-E", Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def PCP_dekadal(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
                latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
                version=3, Waitbar=1):
    """Download WaPOR v3 dekadal precipitation (L1-PCP-D)."""
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    _download_mapset("L1-PCP-D", Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def PCP_monthly(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
                latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
                version=3, Waitbar=1):
    """Download WaPOR v3 monthly precipitation (L1-PCP-M)."""
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    _download_mapset("L1-PCP-M", Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def PCP_yearly(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
               latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
               version=3, Waitbar=1):
    """Download WaPOR v3 annual precipitation (L1-PCP-A)."""
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    _download_mapset("L1-PCP-A", Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def RET_monthly(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
                latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
                version=3, Waitbar=1):
    """Download WaPOR v3 monthly reference ET (L1-RET-M)."""
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    _download_mapset("L1-RET-M", Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def RET_yearly(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
               latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
               version=3, Waitbar=1):
    """Download WaPOR v3 annual reference ET (L1-RET-A)."""
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    _download_mapset("L1-RET-A", Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def AET_dekadal(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
                latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
                level=1, version=3, Waitbar=1):
    """
    Download WaPOR v3 dekadal actual ET (AETI).
    
    Parameters
    ----------
    level : int
        1 for L1-AETI-D (300m continental), 2 for L2-AETI-D (100m national)
    """
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    
    if level == 1:
        mapset = "L1-AETI-D"
    elif level == 2:
        mapset = "L2-AETI-D"
    else:
        raise ValueError(f"Level {level} not supported. Use 1 or 2.")
    
    _download_mapset(mapset, Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def AET_monthly(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
                latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
                level=1, version=3, Waitbar=1):
    """
    Download WaPOR v3 monthly actual ET (AETI).
    
    Parameters
    ----------
    level : int
        1 for L1-AETI-M (300m continental), 2 for L2-AETI-M (100m national)
    """
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    
    if level == 1:
        mapset = "L1-AETI-M"
    elif level == 2:
        mapset = "L2-AETI-M"
    else:
        raise ValueError(f"Level {level} not supported. Use 1 or 2.")
    
    _download_mapset(mapset, Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def AET_yearly(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
               latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
               level=1, version=3, Waitbar=1):
    """
    Download WaPOR v3 annual actual ET (AETI).
    
    Parameters
    ----------
    level : int
        1 for L1-AETI-A (300m continental), 2 for L2-AETI-A (100m national)
    """
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    
    if level == 1:
        mapset = "L1-AETI-A"
    elif level == 2:
        mapset = "L2-AETI-A"
    else:
        raise ValueError(f"Level {level} not supported. Use 1 or 2.")
    
    _download_mapset(mapset, Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def I_dekadal(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
              latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
              level=1, version=3, Waitbar=1):
    """
    Download WaPOR v3 dekadal interception.
    
    Parameters
    ----------
    level : int
        1 for L1-I-D (300m), 2 for L2-I-D (100m)
    """
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    
    if level == 1:
        mapset = "L1-I-D"
    elif level == 2:
        mapset = "L2-I-D"
    else:
        raise ValueError(f"Level {level} not supported. Use 1 or 2.")
    
    _download_mapset(mapset, Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def I_yearly(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
             latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
             level=1, version=3, Waitbar=1):
    """
    Download WaPOR v3 annual interception.
    
    Parameters
    ----------
    level : int
        1 for L1-I-A (300m), 2 for L2-I-A (100m)
    """
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    
    if level == 1:
        mapset = "L1-I-A"
    elif level == 2:
        mapset = "L2-I-A"
    else:
        raise ValueError(f"Level {level} not supported. Use 1 or 2.")
    
    _download_mapset(mapset, Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


def LCC_yearly(Dir, Startdate='2009-01-01', Enddate='2018-12-31',
               latlim=[-40.05, 40.05], lonlim=[-30.5, 65.05],
               level=1, version=3, Waitbar=1):
    """
    Download WaPOR v3 annual land cover classification.
    
    Parameters
    ----------
    level : int
        1 for L1-LCC-A (300m), 2 for L2-LCC-A (100m)
    """
    if version != 3:
        print(f"WARNING: Only version 3 supported. Using version 3.")
    
    if level == 1:
        mapset = "L1-LCC-A"
    elif level == 2:
        mapset = "L2-LCC-A"
    else:
        raise ValueError(f"Level {level} not supported. Use 1 or 2.")
    
    _download_mapset(mapset, Dir, latlim, lonlim, Startdate, Enddate, Waitbar)


# =============================================================================
# Additional helper functions
# =============================================================================

def list_available_mapsets():
    """
    Print all available WaPOR v3 mapsets.
    """
    from waporv3_api import get_mapsets
    
    print("\nAvailable WaPOR v3 Mapsets:")
    print("-" * 80)
    
    mapsets = get_mapsets(include_caption=True)
    for code, caption in mapsets:
        print(f"{code:20s} - {caption}")
    
    print(f"\nTotal: {len(mapsets)} mapsets")


if __name__ == "__main__":
    # Test
    print("WaPOR v3 Module loaded successfully")
    print("\nAvailable functions:")
    funcs = [f for f in dir() if not f.startswith('_') and callable(eval(f))]
    for func in sorted(funcs):
        print(f"  - {func}")