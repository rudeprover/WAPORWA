# -*- coding: utf-8 -*-
"""
Monthly Reference Evapotranspiration from WaPOR v3 (L1-RET-M / L2-RET-M)

This is a v3 replacement of the original v2-based RET_monthly module.
"""

from datetime import datetime, date
import os
import numpy as np
from osgeo import gdal
import urllib.request
import WaPOR
from WaPOR import GIS_functions as gis
from WaPOR.waporv3_api import base_url, collect_responses


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


def main(
    Dir,
    Startdate='2018-01-01',
    Enddate='2024-12-31',
    latlim=[-40.05, 40.05],
    lonlim=[-30.5, 65.05],
    level=1,
    version=3,
    Waitbar=1
):
    """
    Download monthly WaPOR v3 Reference ET for given period and bbox.
    """

   if level == 1:
    mapset_code = 'L1-RET-M'
   elif level == 2:
       print("⚠️ WaPOR v3 does NOT provide L2 RET-M. Using L1-RET-M instead.")
       mapset_code = 'L1-RET-M'
   else:
       print("Only level 1 is available for RET in WaPOR v3.")
    return None

    print(f"\nDownload monthly WaPOR v3 Reference Evapotranspiration data ({mapset_code}) "
          f"for the period {Startdate} till {Enddate}")

    start_dt = datetime.strptime(Startdate, "%Y-%m-%d").date()
    end_dt = datetime.strptime(Enddate, "%Y-%m-%d").date()

    # List all rasters for the mapset
    mapset_url = f"{base_url}/{mapset_code}/layers"

    try:
        all_rasters = collect_responses(mapset_url, info=["code", "downloadUrl"])
    except Exception as e:
        print("ERROR: cannot get list of available data from WaPOR v3")
        print(e)
        return None

    # Filter by date
    selected = []
    for code, meta in all_rasters:
        if "assets" in meta and len(meta["assets"]) > 0:
            url = meta["assets"][0]["href"]
        else:
            url = meta["downloadUrl"]
        print("DEBUG URL =", url)
        dt = _parse_date_from_code(code)
        if dt is None:
            continue
        if start_dt <= dt <= end_dt:
            selected.append((dt, code, url))

    if len(selected) == 0:
        print("No rasters found within requested date range.")
        return None

    selected.sort(key=lambda x: x[0])

    # Output dir
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
                amount, total_amount, prefix='Progress:', suffix='Complete', length=50
            )

    # Bounding box for crop
    bbox = [lonlim[0], latlim[0], lonlim[1], latlim[1]]

    # Loop through rasters
    for dt, code, url in selected:
        print("DEBUG URL =", url)
        fname = f"RET_WAPOR.v3_level{level}_mm-month-1_monthly_{dt.year:04d}.{dt.month:02d}.tif"
        out_path = os.path.join(out_dir, fname)

        if os.path.exists(out_path):
            print("File exists, skipping:", fname)
            if Waitbar == 1 and 'amount' in locals():
                amount += 1
                if WaitbarConsole:
                    WaitbarConsole.printWaitBar(amount, total_amount, prefix='Progress:',
                                                suffix='Complete', length=50)
            continue

        print("Downloading + cropping:", code)

        tmp_raw = os.path.join(out_dir, f"_raw_{code.replace('.', '_')}.tif")
        tmp_path = os.path.join(out_dir, f"_tmp_{code.replace('.', '_')}.tif")

        try:
            # 1) Download raw GeoTIFF
            print("  -> downloading raw file")
            urllib.request.urlretrieve(url, tmp_raw)

            # 2) Crop warp
            warp_opts = gdal.WarpOptions(
                outputBounds=bbox,
                dstNodata=-9999
            )

            src_ds = gdal.Open(tmp_raw)
            if src_ds is None:
                print(f"  !! ERROR: GDAL could not open downloaded file {tmp_raw}")
                continue

            ds = gdal.Warp(tmp_path, src_ds, options=warp_opts)
            src_ds = None

            if ds is None:
                print(f"  !! ERROR: gdal.Warp failed for {code}")
                continue

        finally:
            # Always remove raw
            if os.path.exists(tmp_raw):
                os.remove(tmp_raw)

        # Read, scale, save
        driver, NDV, xsize, ysize, GeoT, Projection = gis.GetGeoInfo(tmp_path)
        arr = gis.OpenAsArray(tmp_path, nan_values=True)

        arr = np.where(np.isnan(arr), NDV, arr)
        arr = np.where(arr < 0, 0, arr)
        arr = arr * SCALE_FACTOR

        gis.CreateGeoTiff(
            out_path, arr.astype("float32"),
            driver, NDV, xsize, ysize, GeoT, Projection
        )

        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        if Waitbar == 1:
            amount += 1
            if WaitbarConsole:
                WaitbarConsole.printWaitBar(amount, total_amount, prefix='Progress:',
                                            suffix='Complete', length=50)

    print(f"\nFinished downloading WaPOR v3 monthly Reference ET ({mapset_code})")
    return out_dir
