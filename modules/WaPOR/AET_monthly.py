# -*- coding: utf-8 -*-
"""
AET_monthly – WaPOR v3 compatibility wrapper

This module keeps the original WaPOR v2-style interface but internally
delegates all work to the new WaPOR v3 implementation in WaPOR_v3.py.

Usage (old style still works):
    import WaPOR
    WaPOR.AET_monthly(Dir=..., Startdate=..., Enddate=..., ...)

Or:
    from WaPOR import AET_monthly
    AET_monthly.main(Dir=..., ...)
"""

from .WaPOR_v3 import AET_monthly as _AET_monthly


def main(Dir,
         Startdate='2009-01-01',
         Enddate='2018-12-31',
         latlim=[-40.05, 40.05],
         lonlim=[-30.5, 65.05],
         level=1,
         version=3,
         Waitbar=1):
    """
    Download WaPOR v3 monthly actual evapotranspiration + interception (AETI).

    Parameters
    ----------
    Dir : str
        Output directory.
    Startdate : str, optional
        Start date in 'YYYY-MM-DD' format. Default '2009-01-01'.
    Enddate : str, optional
        End date in 'YYYY-MM-DD' format. Default '2018-12-31'.
    latlim : list(float, float), optional
        [ymin, ymax] latitude limits of the area of interest.
    lonlim : list(float, float), optional
        [xmin, xmax] longitude limits of the area of interest.
    level : int, optional
        1 -> L1-AETI-M (300 m, continental)
        2 -> L2-AETI-M (100 m, national)
    version : int, optional
        WaPOR version. Only 3 is supported; other values are ignored with a
        warning.
    Waitbar : int, optional
        1 to print progress to screen, 0 for silent.

    Returns
    -------
    None
        Files are written to disk in a subfolder:
        <Dir>/<mapset_code>/Lx-AETI-M.YYYY-MM.tif
    """
    return _AET_monthly(
        Dir=Dir,
        Startdate=Startdate,
        Enddate=Enddate,
        latlim=latlim,
        lonlim=lonlim,
        level=level,
        version=version,
        Waitbar=Waitbar,
    )


# Optional: simple CLI test
if __name__ == "__main__":
    print("Testing AET_monthly wrapper (WaPOR v3)...")
    test_dir = r"D:\Temp\WaPOR_test_AET"
    main(
        Dir=test_dir,
        Startdate="2018-01-01",
        Enddate="2018-01-31",
        latlim=[33.0, 34.0],
        lonlim=[35.0, 36.0],
        level=2,
        version=3,
        Waitbar=1,
    )
    print("Done.")
