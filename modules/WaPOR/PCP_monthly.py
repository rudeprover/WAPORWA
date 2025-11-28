# -*- coding: utf-8 -*-
"""
Monthly precipitation from WaPOR v3 (L1-PCP-M)

This is a v3 replacement of the original v2-based PCP_monthly module.
It:
- Lists rasters from the WaPOR v3 mapset L1-PCP-M
- Filters by Startdate / Enddate
- Crops each raster to the given lat/lon box
- Applies scale factor (0.1) to convert to mm
- Saves GeoTIFFs in a subfolder "<Dir>/L1-PCP-M"

No API token is required.
"""

from datetime import datetime, date
import os

import numpy as np
from osgeo import gdal

import WaPOR       # for WaitbarConsole if present
from WaPOR import GIS_functions as gis
from WaPOR.waporv3_api import base_url, collect_responses


MAPSET_CODE = "L1-PCP-M"
SCALE_FACTOR = 0.1  # multiply raw values to get mm


def _parse_date_from_code(code):
    """
    Extract a date from a WaPOR v3 raster code.

    Examples of codes (exact format may vary):
      WAPOR-3.L1-PCP-M.2018-05
      WAPOR-3_L1-PCP-M_2018-05
      WAPOR-3.L1-PCP-M.2018-05-31

    Returns:
        datetime.date or None if parsing fails.
    """
    # Take the last piece after '.' or '_'
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
    Download monthly WaPOR v3 precipitation (L1-PCP-M) for given period and bbox.

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
    print("DEBUG URL =", url)
    print(f"\nDownload monthly WaPOR v3 precipitation data "
          f"for the period {Startdate} till {Enddate}")

    # Convert date strings to date objects
    start_dt = datetime.strptime(Startdate, "%Y-%m-%d").date()
    end_dt = datetime.strptime(Enddate, "%Y-%m-%d").date()

    # ------------------------------------------------------------------
    # 1. List all rasters for the mapset L1-PCP-M
    # ------------------------------------------------------------------
    mapset_url = f"{base_url}/{MAPSET_CODE}/rasters"
    

    try:
        all_rasters = collect_responses(mapset_url,
                                        info=["code", "downloadUrl"])
    except Exception as e:
        print("ERROR: cannot get list of available data from WaPOR v3")
        print(e)
        return None

    # ------------------------------------------------------------------
    # 2. Filter rasters by date
    # ------------------------------------------------------------------
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

    # Sort by date just for neatness
    selected.sort(key=lambda x: x[0])

    # ------------------------------------------------------------------
    # 3. Prepare output directory
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # 4. Loop over rasters and download + crop + scale
    # ------------------------------------------------------------------
    bbox = [lonlim[0], latlim[0], lonlim[1], latlim[1]]

    for dt, code, url in selected:
        print("DEBUG URL =", url)

        # File naming similar to old v2 convention, but tagged v3
        fname = 'P_WAPOR.v3_mm-month-1_monthly_{:04d}.{:02d}.tif'.format(
            dt.year, dt.month
        )
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

        # Temporary file (cropped COG)
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

        # Remove temporary
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        # Update progress bar
        if Waitbar == 1 and 'amount' in locals():
            amount += 1
            if WaitbarConsole is not None:
                WaitbarConsole.printWaitBar(
                    amount, total_amount,
                    prefix='Progress:',
                    suffix='Complete',
                    length=50
                )

    print("\nFinished downloading WaPOR v3 monthly precipitation")
    return out_dir
