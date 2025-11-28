# -*- coding: utf-8 -*-
"""
Yearly Precipitation from WaPOR v3 (L1-PCP-A)
This script is based on your working PCP_monthly downloader.
"""

from datetime import datetime, date
import os
import numpy as np
from osgeo import gdal
import urllib.request

from WaPOR import GIS_functions as gis
from WaPOR.waporv3_api import base_url, collect_responses


SCALE_FACTOR = 0.1  # Convert raw mm*10 to mm


def _parse_date_from_code(code):
    """Extract a date object from a WaPOR v3 raster code."""
    token = code.split('.')[-1]
    token = token.replace("A", "")

    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            dt = datetime.strptime(token, fmt)
            return date(dt.year, 1, 1)  # yearly → always Jan 1
        except:
            continue
    return None


def main(
    Dir,
    Startdate='2010-01-01',
    Enddate='2023-12-31',
    latlim=[-40, 40],
    lonlim=[30, 45],
    version=3,
    Waitbar=1
):
    print(f"\nDownloading WaPOR v3 Yearly Precipitation (L1-PCP-A) "
          f"from {Startdate} to {Enddate}")

    start_dt = datetime.strptime(Startdate, "%Y-%m-%d").date()
    end_dt = datetime.strptime(Enddate, "%Y-%m-%d").date()

    # Yearly precipitation mapset
    mapset_code = 'L1-PCP-A'
    mapset_url = f"{base_url}/{mapset_code}/rasters"

    # Get list of items
    try:
        all_rasters = collect_responses(mapset_url, info=["code", "downloadUrl"])
    except Exception as e:
        print("❌ ERROR: Cannot reach WaPOR v3 server.")
        print(e)
        return None

    # Filter by date range
    selected = []
    for code, url in all_rasters:
        dt = _parse_date_from_code(code)
        if dt is None:
            continue
        if start_dt <= dt <= end_dt:
            selected.append((dt, code, url))

    if not selected:
        print("❌ No yearly rasters in this time range.")
        return None

    selected.sort()

    # Output folder
    out_dir = os.path.join(Dir, mapset_code)
    os.makedirs(out_dir, exist_ok=True)

    # Bounding box
    bbox = [lonlim[0], latlim[0], lonlim[1], latlim[1]]

    # Loop
    for dt, code, url in selected:
        fname = f"L1-PCP-A_{dt.year}.tif"
        out_path = os.path.join(out_dir, fname)

        print(f"Downloading year {dt.year}: {code}")

        # Temporary files
        tmp_raw = os.path.join(out_dir, f"_raw_{dt.year}.tif")
        tmp_warp = os.path.join(out_dir, f"_warp_{dt.year}.tif")

        try:
            # Download
            urllib.request.urlretrieve(url, tmp_raw)

            # Open raw raster
            src = gdal.Open(tmp_raw)
            if src is None:
                print("❌ GDAL could not open downloaded file.")
                continue
            gt = src.GetGeoTransform()
            xmin = gt[0]
            ymax = gt[3]
            px = gt[1]
            py = gt[5]
            xmax = xmin + src.RasterXSize * px
            ymin = ymax + src.RasterYSize * py
            if lonlim[1] < xmin or lonlim[0] > xmax or latlim[1] < ymin or latlim[0] > ymax:
                print("❌ BBOX outside raster extent — skipping warp for this year.")
                continue


            # Crop
            warp_opts = gdal.WarpOptions(outputBounds=bbox, dstNodata=-9999,warpMemoryLimit=256, multithread=True)
            ds = gdal.Warp(tmp_warp, src, options=warp_opts)

            if ds is None:
                print("❌ gdal.Warp failed for", code)
                continue

        finally:
            if os.path.exists(tmp_raw):
                os.remove(tmp_raw)

        # Read, scale, save
        driver, NDV, xsize, ysize, GeoT, Projection = gis.GetGeoInfo(tmp_warp)
        arr = gis.OpenAsArray(tmp_warp, nan_values=True)

        arr = np.where(np.isnan(arr), NDV, arr)
        arr = np.where(arr < 0, 0, arr)
        arr = arr * SCALE_FACTOR

        gis.CreateGeoTiff(out_path, arr.astype("float32"),
                          driver, NDV, xsize, ysize, GeoT, Projection)

        os.remove(tmp_warp)

    print("\n✔ Finished downloading WaPOR v3 Yearly Precipitation (L1-PCP-A)")
    return out_dir
