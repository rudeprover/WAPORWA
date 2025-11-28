# -*- coding: utf-8 -*-
"""
Yearly Reference Evapotranspiration from WaPOR v3 (L1-RET-A)

This is a v3 replacement of the original v2-based RET_yearly module.
"""

from datetime import datetime, date
import os
import numpy as np
from osgeo import gdal

import WaPOR
from WaPOR import GIS_functions as gis
from WaPOR.waporv3_api import base_url, collect_responses


MAPSET_CODE = "L1-RET-A"
SCALE_FACTOR = 0.1  # multiply raw values to get mm


def _parse_date_from_code(code):
    """Extract a date from a WaPOR v3 raster code."""
    token = code.split('.')[-1]
    token = token.split('_')[-1]

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
         version=3,
         Waitbar=1):
    """
    Download yearly WaPOR v3 Reference ET (L1-RET-A) for given period and bbox.

    Parameters
    ----------
    Dir : str
        Root directory where data will be stored.
    Startdate, Enddate : 'YYYY-MM-DD'
        Date range (inclusive).
    latlim, lonlim : [min, max]
        Latitude and longitude bounds of the area of interest.
    version : int
        Kept for backward compatibility (ignored, always uses v3).
    Waitbar : int (0 or 1)
        If 1, prints a simple textual progress bar.
    """

    print(f"\nDownload yearly WaPOR v3 Reference Evapotranspiration data "
          f"for the period {Startdate} till {Enddate}")

    start_dt = datetime.strptime(Startdate, "%Y-%m-%d").date()
    end_dt = datetime.strptime(Enddate, "%Y-%m-%d").date()

    # List all rasters for the mapset
    mapset_url = f"{base_url}/{MAPSET_CODE}/rasters"

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
    out_dir = os.path.join(Dir, MAPSET_CODE)
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
        fname = 'RET_WAPOR.v3_mm-year-1_annually_{:04d}.tif'.format(dt.year)
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

    print("\nFinished downloading WaPOR v3 yearly Reference ET")
    return out_dir