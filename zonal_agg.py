import json
import numpy as np
import xarray as xr
import geopandas as gpd
from typing import Tuple
from affine import Affine
from rasterio import features


def load_data(file_path: str, indicator_code: str) -> Tuple:
    """
    Load historical climate data and provincial geopolygons, and set CRS properly.

    Args:
        file_path (str): Path to the NetCDF file containing historical data.
        indicator_code (str): Variable name/code to extract from the dataset.

    Returns:
        Tuple: A tuple containing:
            - historical_data (xarray.DataArray): The climate variable data with CRS set.
            - geopolygon (GeoDataFrame): Provincial boundaries with CRS set.
    """

    geocode_path = 'utils/PH_GEOCODE.json'
    with open(geocode_path, 'r', encoding='utf-8') as f:
        ph_geomap = json.load(f)

    province_geo_map = ph_geomap.get('Provincial Geo Map')
    # Convert the province geometries into a GeoDataFrame
    geopolygon = gpd.GeoDataFrame.from_features(province_geo_map["features"])
    # Set the coordinate reference system (CRS) to WGS84 (EPSG:4326)
    geopolygon.set_crs("EPSG:4326", inplace=True)

    historical_data = xr.open_dataset(file_path)
    historical_data = historical_data[indicator_code]
    # Assign the same CRS of WGS84 (EPSG:4326) to the historical data using the rioxarray extension
    historical_data.rio.write_crs("EPSG:4326", inplace=True) 

    return historical_data, geopolygon

def zonal_aggregation(historical_data: xr.Dataset, geopolygons: gpd.GeoDataFrame) -> Tuple:
    """
    Perform zonal aggregation (mean, min, max, and total) across each geopolygon.

    Args:
        historical_data (xarray.DataArray): The climate variable data with CRS set.
        geopolygons (GeoDataFrame): Provincial boundaries with CRS set.

    Returns:
        Tuple: A tuple containing:
            - zonal_means (List[float]): Mean values computed for each zone.
            - zonal_maximums (List[float]): Maximum values computed for each zone.
            - zonal_minimums (List[float]): Minimum values computed for each zone.
            - zonal_totals (List[float]): Total (sum) values computed for each zone.
    """
    # Create affine transform to map pixel coordinates to spatial coordinates
    transform = Affine.translation(
        historical_data['longitude'][0].item(), # Starting longitude
        historical_data['latitude'][0].item()   # Starting latitude
    ) * Affine.scale(
        historical_data['longitude'][1].item() - historical_data['longitude'][0].item(), # Pixel width
        historical_data['latitude'][1].item() - historical_data['latitude'][0].item()    # Pixel height
    )

    # Prepare geometries and assign a unique integer ID to each polygon
    shapes = [(geom, i + 1) for i, geom in enumerate(geopolygons.geometry)]  

    # Rasterize the polygons to create a mask array where each pixel has the polygon ID it belongs to
    mask = features.rasterize(
        shapes=shapes,
        out_shape=(len(historical_data['latitude']), len(historical_data['longitude'])),
        transform=transform,
        fill=0,
        all_touched=True,
        dtype=np.int32
    )
    # Expand the 2D mask to match the full data shape (e.g., time, lat, lon) for element-wise operations
    mask_exp = np.broadcast_to(mask, historical_data.shape) 

    # Create a DataArray from the mask to apply masking using xarray
    masker = xr.DataArray(mask_exp, coords=historical_data.coords, dims=historical_data.dims)

    zonal_means = []
    zonal_maximums = []
    zonal_minimums = []
    zonal_totals = []

    for polygon_id in range(1, len(geopolygons) + 1):
        # Create a boolean mask for the current polygon
        polygon_mask = (masker == polygon_id)

        # Apply the mask to the historical data
        values = historical_data.where(polygon_mask)

        # Compute zonal statistics
        mean_values = values.mean().item()  
        maximum_values = values.max().item()
        minimum_values = values.min().item()
        total_values = values.sum().item()

        zonal_means.append(mean_values)
        zonal_maximums.append(maximum_values)
        zonal_minimums.append(minimum_values)
        zonal_totals.append(total_values)

    return zonal_means, zonal_maximums, zonal_minimums, zonal_totals


def daily_aggregation(historical_data: xr.Dataset, geopolygons: gpd.GeoDataFrame) -> None:
    """
    Perform daily aggregation of climate data using zonal statistics.

    For each day in the dataset, compute zonal mean, max, min, and total
    values across the provided geopolygons and print the results.

    Args:
        historical_data (xarray.DataArray): The climate variable data with CRS set.
        geopolygons (GeoDataFrame): Provincial boundaries with CRS set.

    """
    # Create a copy of the input geopolygons to store daily statistics
    geopolygons_daily = geopolygons.copy()
    # Group the data by day using the 'valid_time' coordinate's date part
    daily_groups = historical_data.groupby('valid_time.date')

    for day, daily_data in daily_groups:
        # Compute zonal statistics (mean, max, min, total) for the day
        zonal_means, zonal_maximums, zonal_minimums, zonal_totals = zonal_aggregation(daily_data, geopolygons)

        # Add a new column with the date
        geopolygons_daily['date'] = day

        # Add computed zonal statistics to the GeoDataFrame
        geopolygons_daily['mean_value'] = zonal_means
        geopolygons_daily['max_value'] = zonal_maximums
        geopolygons_daily['min_value'] = zonal_minimums
        geopolygons_daily['total_value'] = zonal_totals

        print(geopolygons_daily[['date', 'location', 'mean_value', 'max_value', 'min_value', 'total_value']])


if __name__ == "__main__":
    data, polygon_set = load_data(file_path="downloads/2m_temperature/1980-06.nc", indicator_code="t2m")
    daily_aggregation(data, polygon_set)