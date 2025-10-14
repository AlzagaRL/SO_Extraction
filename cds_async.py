import os
import glob
import json
import cdsapi
import asyncio
import logging
import xarray as xr
from typing import Dict
from pathlib import Path
from functools import partial
from dotenv import load_dotenv
from datetime import datetime, timedelta


class CDSAPIClient:
    """ A client for CDS API data extraction. """
    def __init__(self, config: Dict):
        """
        Initializes the client by setting configuration-dependent attributes,
        defining the time range for data retrieval, and ensuring the local
        download path exists.

        Args:
            config (Dict): The config set up containing the attributes.
        """
        self.days = None
        self.months = None
        self.years = None
        self.hours = None
        self.logger = None
        self.config = config
        self.path = config.get('path')
        self.start = config.get('date_control').get('start')
        self.end = config.get('date_control').get('end')
        self.latest = config.get('date_control').get('end') - timedelta(days=config.get('date_control').get('delay'))
        self.variables = self.config.get('variables')
        self.bounding_box = self.config.get('bounding_box')

    def scope_years(self):
        """
        Sets the list of unique years between the `start` and `latest` dates.

        The method populates the `self.years` attribute with a sorted, unique list
        of years from the date range, formatted as a string (e.g., ['2012', '2013']).
        """
        end = self.latest.replace(hour=0, minute=0, second=0, microsecond=0)
        date_list = [self.start + timedelta(days=x) for x in range((end - self.start).days + 1)]
        self.years = sorted(list(set([d.strftime('%Y') for d in date_list])))

    def scope_date(self, year: int):
        """
        Sets the valid months and days for a given year based on the
        configured start and end dates.

        Args:
            year (int): The calendar year for which to set the date scope.
        """

        # If the current year is the latest year, set the end date to today.
        # Otherwise, set the end date to the last day of the year.
        if str(self.latest.year) == year:
            # The earliest date in the current year is either the configured start date
            # or Jan 1st, whichever is later.
            start = datetime(int(year), 1, 1)
            if start < self.start:
                start = self.start
            
            # The end date is today, with the time removed.
            end = self.latest.replace(hour=0, minute=0, second=0, microsecond=0)
            date_list = [start + timedelta(days=x) for x in range((end - start).days + 1)]
        else:
            # For a historical year, the range is from the configured start date
            # until the end of that year.
            end = datetime(int(year), 12, 31)
            date_list = [self.start + timedelta(days=x) for x in range((end - self.start).days + 1)]
            
        self.hours = [str(h).zfill(2) for h in range(0, 24)]
        self.days = sorted(list(set([d.strftime('%d') for d in date_list])))
        self.months = sorted(list(set([d.strftime('%m') for d in date_list])))

    async def fetch(self):
        """
        Asynchronously fetches data from the CDS API by offloading blocking
        requests to a separate thread pool. Requests are chunked by climate
        indicators, years, and months for concurrent processing.
        """
        load_dotenv()
        cds_key = os.getenv("CDS_Key")
        client = cdsapi.Client(
            url="https://cds.climate.copernicus.eu/api",
            key=cds_key,
        )
        loop = asyncio.get_running_loop()
        tasks = []

        self.logger.info(f"Starting to fetch from the CDS API...")
        for variable in self.variables:
            self.logger.info(f"Extraction of historical data for {variable}...")
            Path(f"{self.path}/{variable}").mkdir(exist_ok=True)
            for year in self.years:
                self.scope_date(year)
                for month in self.months:
                    # Offload the blocking `client.retrieve` call to a separate thread.
                    # This is necessary because the cdsapi client is not asyncio-native
                    try:
                        request_call = partial(
                            client.retrieve,
                            self.config.get('base'),
                            {
                                'product_type': self.config.get('product_type'),
                                'variable': variable,
                                'year': year,
                                'month': month,
                                'day': self.days,
                                'time': self.hours,
                                'area': [
                                    self.bounding_box.get('north'),
                                    self.bounding_box.get('west'),
                                    self.bounding_box.get('south'),
                                    self.bounding_box.get('east'),
                                ],
                                'format': self.config.get('format')
                            },
                            Path(self.path) / variable / f"{year}-{month}.nc"
                        )
                        
                        task = loop.run_in_executor(None, request_call)
                        tasks.append(task)
                        await asyncio.sleep(5)
                        self.logger.info(f"Starting to process data extraction for chunk {year}-{month}")

                    except Exception as e:
                        self.logger.error(f"Fail to process data extraction for chunk {year}-{month}:", e)

        # Wait for all the asynchronous retrieval tasks to complete.            
        await asyncio.gather(*tasks)
        self.logger.info("Extraction from the API is complete!")

    def post_fetch(self):
        """
        Process all downloaded chunk files for each variable. For each variable:
        - Aggregate data daily by calculating the mean, minimum, and maximum values.
        - Interpolate data for each province and save the results in NetCDF and CSV formats.
        """
        try:
            with open('utils/ph_province_coordinates.json', 'r', encoding='utf-8') as f:
                # Generated from Nominatim OpenStreetMap API <geolocate.py>
                provinces = json.load(f)
            self.logger.info("Successfully load province coordinates lookup!")
        except Exception as e:
            self.logger.error("Failed to load province coordinates lookup:", e)
            return
        
        xr.set_options(use_new_combine_kwarg_defaults=True)

        for variable in self.variables:
            file_pattern = Path(self.path) / variable / '*.nc'
            file_paths = glob.glob(str(file_pattern))
            dataset = []

            for file_path in file_paths:
                try:
                    file = xr.open_dataset(file_path)
                    dataset.append(file)
                except Exception as e:
                    self.logger.warning(f"File in path {file_path} is either empty or corrupted, skipping.")
                    continue

            try:
                complete_dataset =  xr.concat(dataset, dim='valid_time')
                daily_minimums = complete_dataset.resample(valid_time='1D').min()
                daily_maximums = complete_dataset.resample(valid_time='1D').max()
                daily_means = complete_dataset.resample(valid_time='1D').mean()

                complete_dataset_daily_aggregated = xr.Dataset({
                    "daily_min": daily_minimums.to_array(),
                    "daily_max": daily_maximums.to_array(),
                    "daily_mean": daily_means.to_array()
                })

                self.logger.info("Successfully aggregated hourly data into daily data of mean, minimum and maximum.")
                
                provinces_records = []
                for province in provinces:
                    province_record = complete_dataset_daily_aggregated.interp(latitude=province['Latitude'], longitude=province['Longitude'], method="linear")
                    province_record = province_record.assign_coords(location_name=province['Location'])
                    provinces_records.append(province_record)

                self.logger.info("Successfully interpolated the value for all provinces!")

                all_provinces_record = xr.concat(provinces_records, dim='location_name')
                all_provinces_record.to_netcdf(f'{variable}.nc')
                all_provinces_record.to_dataframe().reset_index().to_csv(f'{variable}.csv')

                self.logger.info(f"Dataset for {variable} successfully saved!")

            except Exception as e:
                self.logger.error("Failed to process downloaded files for daily aggregations and file saving:", e)

    def pre_fetch(self):
        """
        Initializes the pre-featch process by ensuring the output directory exists creation, configured logger,
        and chuking years for api request.
        """
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - [%(name)s] - [%(funcName)s] - %(levelname)s - %(message)s',
        )
        Path(self.path).mkdir(exist_ok=True)
        self.scope_years()
        self.logger = logging.getLogger(__name__)

    async def launch(self):
        """
        Proceeds to pre-fetch for configurations set up, fetch for asynchronous extraction from the api 
        and post-fetch for data processing into an output file
        """
        self.pre_fetch()
        await self.fetch()
        self.post_fetch()


if __name__ == "__main__":
    config = {
        'date_control' : {
            'start': datetime(2025, 8, 1),
            'end': datetime.now(),
            'delay': 5, 
            # 5 days is the API delay from real time
        },
        # Generated from Nominatim OpenStreetMap API <bbox.py>
        'bounding_box' : {
            "west": 114.1003696,
            "south": 4.3833333,
            "east": 126.803083,
            "north": 21.321928
        },
        # CDS variables: https://earth.bsc.es/gitlab/dtrujill/c3s512-wp1-datachecker/-/blame/573cbc3a5015b0a84a5fb5ce7f230c8d792d284a/cds_metadata/cds_variables_20190404.json
        'variables': ['surface_pressure'],
        'product_type': 'reanalysis',
        'base': 'reanalysis-era5-single-levels',
        'format': 'netcdf',
        'path': 'downloads'
    }

    cds = CDSAPIClient(config)
    asyncio.run(cds.launch())

