import os
import glob
import time
import cdsapi
import asyncio
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
        self.config = config
        self.path = config['path']
        self.start = config['date_control']['start']
        self.end = config['date_control']['today']
        self.latest = config['date_control']['today'] - timedelta(days=self.config['date_control']['delay'])
        # Path(self.path).mkdir(exist_ok=True)

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

    async def call(self):
        """
        Asynchronously fetches data from the CDS API by offloading blocking
        requests to a separate thread pool. Requests are chunked by climate
        indicators, years, and months for concurrent processing.
        """
        load_dotenv()
        tasks = []
        loop = asyncio.get_running_loop()
        cds_key = os.getenv("CDS_Key")
        client = cdsapi.Client(
            url="https://cds.climate.copernicus.eu/api",
            key=cds_key,
            # quiet=False, 
            # debug=True
        )

        for variable in self.config['variables']:
            Path(f"{self.path}/{variable}").mkdir(exist_ok=True)
            for year in self.years:
                self.scope_date(year)
                for month in self.months:
                    # Offload the blocking `client.retrieve` call to a separate thread.
                    # This is necessary because the cdsapi client is not asyncio-native
                    try:
                        request_call = partial(
                            client.retrieve,
                            self.config['base'],
                            {
                                'product_type': self.config['product_type'],
                                'variable': variable,
                                'year': year,
                                'month': month,
                                'day': self.days,
                                'time': self.hours,
                                'area': [
                                    self.config['bounding_box']['north'], 
                                    self.config['bounding_box']['west'], 
                                    self.config['bounding_box']['south'], 
                                    self.config['bounding_box']['east'], 
                                ],
                                'format': self.config['format']
                            },
                            Path(self.path) / variable / f"{year}-{month}.nc"
                        )
                        
                        task = loop.run_in_executor(None, request_call)
                        tasks.append(task)
                        await asyncio.sleep(5)

                    except Exception as e:
                        print(f"Fail to process extraction of data for {year}-{month}:", e)


        # Wait for all the asynchronous retrieval tasks to complete.            
        await asyncio.gather(*tasks)


    def convert_daily(self):
        """ Convert hourly data into daily aggregations of mean, max and min """

        entire_dataset = []
        for year in self.years:
            file_pattern = Path(self.path) / self.config['variables'][0] / f'{year}-*.nc'
            file_paths = glob.glob(str(file_pattern))
            #print(file_pattern)
            monthly_aggregated = []
            for file_path in file_paths:
                file = xr.open_dataset(file_path)
                daily_mean = file.resample(valid_time='1D').mean()
                daily_minimum = file.resample(valid_time='1D').min()
                daily_maximum = file.resample(valid_time='1D').max()
                
                daily_aggregated = xr.Dataset({
                    "daily_mean": daily_mean.to_array(),
                    "daily_min": daily_minimum.to_array(),
                    "daily_max": daily_maximum.to_array()
                })
                #print(daily_aggregated)
                #daily_aggregated = daily_aggregated.interp(latitude=13.216667, longitude=123.55)
                #print(daily_aggregated.to_dataframe().reset_index())
                monthly_aggregated.append(daily_aggregated)
                #print(daily_maximum)
            yearly_aggregated = xr.concat(monthly_aggregated, dim='valid_time')
            entire_dataset.append(yearly_aggregated)

        complete_dataset = xr.concat(entire_dataset, dim='valid_time')

        # Test # Use spline for interp method?
        complete_dataset = complete_dataset.interp(latitude=13.216667, longitude=123.55)
        complete_dataset = complete_dataset.assign(loc_name="location")
        print(complete_dataset.to_dataframe().reset_index())

    def log(self, text: str):
        print("=" * 7)
        print(text)
        print("=" * 7)

    def concat_all(self):
        print("CONCAT all files *.nc in the variable/indicator directory")
        ds = xr.open_mfdataset(Path(self.path) / self.config['variables'][0] / '*.nc', concat_dim='valid_time')
        
        ds.to_netcdf(Path(self.path) / f'{self.config['variables'][0]}.concatenated_data.nc')

    async def main(self):
        Path(self.path).mkdir(exist_ok=True)
        self.scope_years()
        await self.call()
        self.convert_daily()


config = {
    'date_control' : {
        'start': datetime(2012, 5, 1),
        'today': datetime.now(),
        #'today': datetime(2013, 1, 2),
        'delay': 5, 
        # 5 days is the API delay from real time
    },
    'filename_aggregations': {
        'mean': 'phillipines_daily_mean.nc', 
        'maximum' : 'phillipines_daily_max.nc', 
        'minimum': 'phillipines_daily_min.nc'
    },
    # Github source for PH Bounding Box <https://gist.github.com/graydon/11198540>
    'bounding_box' : {
        "west": 117.17427453,
        "south": 5.58100332277,
        "east": 126.537423944,
        "north": 18.5052273625
    },
    #'variables': ['2m_temperature', 'total_precipitation', 'surface_pressure'],
    'variables': ['total_precipitation'],
    'product_type': 'reanalysis',
    'base': 'reanalysis-era5-single-levels',
    'format': 'netcdf',
    'filename': '2m_temperature.nc',
    'path': 'downloads'
}

cds = CDSAPIClient(config)
asyncio.run(cds.main())


