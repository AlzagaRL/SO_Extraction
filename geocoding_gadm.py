import pandas as pd
import numpy as np
from gadm import GADMDownloader
from pathlib import Path
import json
# pip install shapely==1.8.0
# pip install fiona==1.9.6

def monkey_patch():
    """ Monkey patch for module compatability and prevent issues. """
    # Monkey patch for compatibility with older geopandas/fiona
    if not hasattr(pd, 'Int64Index'):
        pd.Int64Index = pd.Index

    # Monkey patch for np.array to fallback to np.asarray if error occurs
    _orig_array = np.array
    def safe_array(obj, *args, **kwargs):
        try:
            return _orig_array(obj, *args, **kwargs)
        except ValueError as e:
            if "Unable to avoid copy" in str(e):
                return np.asarray(obj)
            else:
                raise
    np.array = safe_array

def get_geocoding():
    """ Get Bounding Box of the Philippines and Geo Map for every provinces and store it in dictionary. """
    downloader = GADMDownloader()

    country_gdf = downloader.get_shape_data_by_country_name(country_name="Philippines", ad_level=0)
    west, south, east, north = country_gdf.total_bounds
    bounding_box = {
        "west": west,
        "south": south,
        "east": east,
        "north": north
    }

    province_gdf = downloader.get_shape_data_by_country_name(country_name="Philippines", ad_level=1)
    province_gdf = province_gdf.to_crs(epsg=4326)
    province_gdf['longitude'] = province_gdf.geometry.centroid.x
    province_gdf['latitude'] = province_gdf.geometry.centroid.y
    province_gdf = province_gdf[['NAME_1', 'longitude', 'latitude', 'geometry']]
    province_gdf = province_gdf.rename(columns={"NAME_1": "location"})
    geo_map = json.loads(province_gdf.to_json())

    return bounding_box, geo_map


if __name__ == "__main__":
    monkey_patch()
    bounding_box, geo_map = get_geocoding()
    ph_geocode = {
        "Bounding Box" : bounding_box,
        "Provincial Geo Map": geo_map
    }

    with open(Path("utils") / "PH_GEOCODE.json", "w") as f:
        json.dump(ph_geocode, f, indent=4)  

