# -*- coding: utf-8 -*-
"""
WaPOR v3 API Helper Functions
Based on official FAO WaPOR v3 tutorial
No authentication required - uses public COG files
"""

import requests
import time

# Base URL for WaPOR v3
BASE_URL = "https://data.apps.fao.org/gismgr/api/v2/catalog/workspaces/WAPOR-3/mapsets"
base_url = BASE_URL  # backward compatibility with older modules


def collect_responses(url, info=["code"]):
    """
    Collect paginated API responses from WaPOR v3.
    Handles pagination using 'links' with rel='next'.
    
    Parameters
    ----------
    url : str
        Initial URL to query
    info : list or str
        List of field names to extract from each item, or "all" for complete items
        
    Returns
    -------
    list
        If info is a list: returns list of tuples with requested fields
        If info is "all": returns list of complete item dictionaries
        
    Example
    -------
    >>> mapsets = collect_responses(BASE_URL, info=["code", "caption"])
    >>> rasters = collect_responses(mapset_url, info=["code", "downloadUrl"])
    """
    # Initialize with a dummy "next" link to start the loop
    data = {"links": [{"rel": "next", "href": url}]}
    output = []
    
    while "next" in [x["rel"] for x in data.get("links", [])]:
        # Get the URL for the next page
        url_ = [x["href"] for x in data["links"] if x["rel"] == "next"][0]
        
        # Make request with retry logic
        for attempt in range(3):
            try:
                response = requests.get(url_, timeout=30)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt == 2:
                    print(f"Error fetching data after 3 attempts: {e}")
                    return output
                time.sleep(2)
        
        # Parse response - WaPOR v3 nests data under "response" key
        json_data = response.json()
        data = json_data.get("response", {})
        
        # Extract items
        items = data.get("items", [])
        if not items:
            break
            
        # Extract requested fields from each item
        if info == "all":
            output.extend(items)
        elif isinstance(info, list):
            for item in items:
                record = tuple(item.get(field) for field in info)
                output.append(record)
        
    # Sort if we extracted specific fields
    if isinstance(info, list) and info != "all":
        output = sorted(output)
        
    return output


def get_mapsets(include_caption=True):
    """
    Get list of all available WaPOR v3 mapsets.
    
    Parameters
    ----------
    include_caption : bool
        If True, return (code, caption) tuples. If False, return only codes.
        
    Returns
    -------
    list
        List of mapset codes or (code, caption) tuples
        
    Example
    -------
    >>> mapsets = get_mapsets()
    >>> # [('L1-AETI-D', 'Actual Evapotranspiration...'), ...]
    """
    if include_caption:
        return collect_responses(BASE_URL, info=["code", "caption"])
    else:
        return collect_responses(BASE_URL, info=["code"])


def get_rasters(mapset_code, include_url=True):
    """
    Get list of all rasters in a specific mapset.
    
    Parameters
    ----------
    mapset_code : str
        Mapset code (e.g., 'L1-PCP-E', 'L2-AETI-M')
    include_url : bool
        If True, return (code, downloadUrl) tuples. If False, return only codes.
        
    Returns
    -------
    list
        List of raster codes or (code, url) tuples
        
    Example
    -------
    >>> rasters = get_rasters('L1-PCP-E')
    >>> # [('L1-PCP-E.2020-01-01', 'https://...'), ...]
    """
    mapset_url = f"{BASE_URL}/{mapset_code}/rasters"
    
    if include_url:
        return collect_responses(mapset_url, info=["code", "downloadUrl"])
    else:
        return collect_responses(mapset_url, info=["code"])


def get_raster_info(mapset_code):
    """
    Get complete information for all rasters in a mapset.
    Includes metadata like bbox, temporal coverage, etc.
    
    Parameters
    ----------
    mapset_code : str
        Mapset code (e.g., 'L1-PCP-E')
        
    Returns
    -------
    list
        List of dictionaries with complete raster information
    """
    mapset_url = f"{BASE_URL}/{mapset_code}/rasters"
    return collect_responses(mapset_url, info="all")


def filter_rasters_by_date(rasters, start_date, end_date):
    """
    Filter rasters by date range based on their code.
    
    Parameters
    ----------
    rasters : list
        List of (code, url) tuples from get_rasters()
    start_date : str
        Start date in format 'YYYY-MM-DD'
    end_date : str
        End date in format 'YYYY-MM-DD'
        
    Returns
    -------
    list
        Filtered list of (code, url) tuples
        
    Example
    -------
    >>> rasters = get_rasters('L1-PCP-E')
    >>> filtered = filter_rasters_by_date(rasters, '2020-01-01', '2020-12-31')
    """
    from datetime import datetime
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    filtered = []
    for code, url in rasters:
        # Extract date from code
        # Formats: YYYY-MM-DD (daily), YYYY-MM-D1/D2/D3 (dekadal), YYYY-MM (monthly), YYYY (annual)
        try:
            # Try daily format first (e.g., L1-PCP-E.2020-01-01)
            if '.' in code:
                date_str = code.split('.')[-1]
                
                # Handle dekadal (YYYY-MM-D1)
                if date_str.count('-') == 2 and date_str[-2:].startswith('D'):
                    date_str = date_str[:-3]  # Remove -D1 part
                    raster_date = datetime.strptime(date_str, '%Y-%m')
                # Handle monthly (YYYY-MM)
                elif date_str.count('-') == 1:
                    raster_date = datetime.strptime(date_str, '%Y-%m')
                # Handle annual (YYYY)
                elif '-' not in date_str and len(date_str) == 4:
                    raster_date = datetime.strptime(date_str, '%Y')
                # Handle daily (YYYY-MM-DD)
                else:
                    raster_date = datetime.strptime(date_str, '%Y-%m-%d')
                
                if start <= raster_date <= end:
                    filtered.append((code, url))
        except (ValueError, IndexError):
            # If date parsing fails, include it anyway
            print(f"Warning: Could not parse date from code: {code}")
            filtered.append((code, url))
    
    return filtered


if __name__ == "__main__":
    # Test the functions
    print("Testing WaPOR v3 API...")
    
    # Test 1: Get mapsets
    print("\n1. Getting available mapsets...")
    mapsets = get_mapsets()
    print(f"Found {len(mapsets)} mapsets")
    print("First 5 mapsets:", mapsets[:5])
    
    # Test 2: Get rasters for a specific mapset
    print("\n2. Getting rasters for L1-PCP-E...")
    rasters = get_rasters('L1-PCP-E')
    print(f"Found {len(rasters)} rasters")
    if rasters:
        print("First raster:", rasters[0])
    
    # Test 3: Filter by date
    print("\n3. Filtering rasters for 2020...")
    filtered = filter_rasters_by_date(rasters, '2020-01-01', '2020-12-31')
    print(f"Found {len(filtered)} rasters in 2020")