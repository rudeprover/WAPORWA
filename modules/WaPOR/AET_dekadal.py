# -*- coding: utf-8 -*-
"""
Dekadal Actual Evapotranspiration from WaPOR v3 (L1-AETI-D / L2-AETI-D)

This is a v3 replacement of the original v2-based AET_dekadal module.
Note: In WaPOR v3, this is called AETI (Actual Evapotranspiration and Interception).
"""

from datetime import datetime, date
import os
import numpy as np
from osgeo import gdal

import WaPOR
from WaPOR import GIS_functions as gis
from WaPOR.waporv3_api import base_url, collect_responses


SCALE_FACTOR = 0.1  # multiply raw values to get mm


def _parse_date_from_code(code):
    """Extract a date from a WaPOR v3 raster code."""
    token = code.split('.')[-1]
    token = token.split('_')[-1]

    # Handle dekadal format
    if 'D' in token[-2:]:
        token = token[:-3]
    
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            dt = datetime.strptime(token, fmt)
            if fmt == "%Y":
                return date(dt.year, 1, 1)
            elif fmt == "%Y-%m":
                return date(dt.year, dt.month, 1)
            else:
                return dt.date()
        except ValueError:
            continue
    return None


def main(Dir,
         Startdate='2018-01-01',
         Enddate='2024-12-31',
         latlim=[-40.05, 40.05],
         lonlim=[-30.5, 65.05],
         level=1,
         version=3,
         Waitbar=1):
    """
    Download dekadal WaPOR v3 Actual ET (AETI) for given period and bbox.

    Parameters
    ----------
    Dir : str
        Root directory where data will be stored.
    Startdate, Enddate : 'YYYY-MM-DD'
        Date range (inclusive).
    latlim, lonlim : [min, max]
        Latitude and longitude bounds of the area of interest.
    level : int
        1 for L1-AETI-D (continental), 2 for L2-AETI-D (national)
    version : int
        Kept for backward compatibility (ignored, always uses v3).
    Waitbar : int (0 or 1)
        If 1, prints a simple textual progress bar.
    """

    if level == 1:
        mapset_code = 'L1-AETI-D'
    elif level == 2:
        mapset_code = 'L2-AETI-D'
    else:
        print('This module only supports level 1 and level 2 data.')
        return None

    print(f"\nDownload dekadal WaPOR v3 Actual ET (AETI) data ({mapset_code}) "
          f"for the period {Startdate} till {Enddate}")

    start_dt = datetime.strptime(Startdate, "%Y-%m-%d").date()
    end_dt = datetime.strptime(Enddate, "%Y-%m-%d").date()

    # List all rasters for the mapset
    mapset_url = f"{base_url}/{mapset_code}/rasters"

    try:
        all_rasters = collect_responses(mapset_url,
                                        info=["code", "downloadUrl"])
    except Exception as e:
        print("ERROR: cannot get list of available data from WaPOR v3")
        print(e)
        return None

    # Filter rasters by date
    selected = []
    for code, url in all_rasters:
        dt = _parse_date_from_code(code)
        if dt is None:
            continue
        if (dt >= start_dt) and (dt <= end_dt):
            selected.append((dt, code, url))

    if len(selected) == 0:
        print("No rasters found within requested date range.")
        return None

    selected.sort(key=lambda x: x[0])

    # Prepare output directory
    out_dir = os.path.join(Dir, mapset_code)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Progress bar
    if Waitbar == 1:
        try:
            import WaPOR.WaitbarConsole as WaitbarConsole
        except ImportError:
            WaitbarConsole = None
        total_amount = len(selected)
        amount = 0
        if WaitbarConsole is not None:
            WaitbarConsole.printWaitBar(
                amount, total_amount,
                prefix='Progress:',
                suffix='Complete',
                length=50
            )

    # Loop over rasters
    bbox = [lonlim[0], latlim[0], lonlim[1], latlim[1]]

    for dt, code, url in selected:
        raster_id = code.split('.')[-1] if '.' in code else code
        fname = f'AETI_WAPOR.v3_level{level}_mm-dekad-1_{raster_id}.tif'
        out_path = os.path.join(out_dir, fname)

        if os.path.exists(out_path):
            print("File exists, skipping:", fname)
            if Waitbar == 1 and 'amount' in locals():
                amount += 1
                if WaitbarConsole is not None:
                    WaitbarConsole.printWaitBar(
                        amount, total_amount,
                        prefix='Progress:',
                        suffix='Complete',
                        length=50
                    )
            continue

        print("Downloading + cropping:", code)

        tmp_path = os.path.join(out_dir, "_tmp_{}.tif".format(code.replace('.', '_')))

        warp_opts = gdal.WarpOptions(
            outputBounds=bbox,
            dstNodata=-9999
        )
        gdal.Warp(tmp_path, f"/vsicurl/{url}", options=warp_opts)

        # Read, scale, save
        driver, NDV, xsize, ysize, GeoT, Projection = gis.GetGeoInfo(tmp_path)
        arr = gis.OpenAsArray(tmp_path, nan_values=True)

        arr = np.where(np.isnan(arr), NDV, arr)
        arr = np.where(arr < 0, 0, arr)
        arr = arr * SCALE_FACTOR

        gis.CreateGeoTiff(out_path, arr.astype("float32"),
                          driver, NDV, xsize, ysize, GeoT, Projection)

        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        if Waitbar == 1 and 'amount' in locals():
            amount += 1
            if WaitbarConsole is not None:
                WaitbarConsole.printWaitBar(
                    amount, total_amount,
                    prefix='Progress:',
                    suffix='Complete',
                    length=50
                )

    print(f"\nFinished downloading WaPOR v3 dekadal Actual ET ({mapset_code})")
    return out_dir