import os
import cdsapi
import asyncio
import logging
from typing import Dict
from pathlib import Path
from functools import partial
from dotenv import load_dotenv
from datetime import datetime, timedelta


class CDSAPIClient:
    """ A client for CDS API data extraction. """
    def __init__(self, config: Dict) -> None:
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

    def scope_years(self) -> None:
        """
        Sets the list of unique years between the `start` and `latest` dates.

        The method populates the `self.years` attribute with a sorted, unique list
        of years from the date range, formatted as a string (e.g., ['2012', '2013']).
        """
        end = self.latest.replace(hour=0, minute=0, second=0, microsecond=0)
        date_list = [self.start + timedelta(days=x) for x in range((end - self.start).days + 1)]
        self.years = sorted(list(set([d.strftime('%Y') for d in date_list])))

    def scope_date(self, year: int) -> None:
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

    async def fetch(self) -> None:
        """
        Asynchronously fetches data from the CDS API by offloading blocking
        requests to a separate thread pool. Requests are chunked by climate
        indicators, years, and months for concurrent processing.
        """
        bounding_box = {
            "west": 116.92833709800016,
            "south": 4.586940000000141,
            "east": 126.60534668000014,
            "north": 21.070141000000092
        }
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
                            self.config.get('dataset_name'),
                            {
                                'product_type': self.config.get('product_type'),
                                'variable': variable,
                                'year': year,
                                'month': month,
                                'day': self.days,
                                'time': self.hours,
                                'area': [
                                    bounding_box.get('north'),
                                    bounding_box.get('west'),
                                    bounding_box.get('south'),
                                    bounding_box.get('east'),
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

    def prep(self) -> None:
        """
        Prepares for the data fetching process by setting up the output/download directory,
        configuring the logger, and dividing the years into chunks for API requests
        """
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - [%(name)s] - [%(funcName)s] - %(levelname)s - %(message)s',
        )
        Path(self.path).mkdir(exist_ok=True)
        self.scope_years()
        self.logger = logging.getLogger(__name__)

    async def launch(self) -> None:
        """
        Performs pre-fetch setup and asynchronously processes the data fetch request.
        """
        self.prep()
        await self.fetch()


if __name__ == "__main__":
    config = {
        'date_control' : {
            'start': datetime(1950, 1, 1),
            'end': datetime.now(),
            'delay': 5, 
            # 5 days is the API delay from real time
        },
        # CDS variables: https://earth.bsc.es/gitlab/dtrujill/c3s512-wp1-datachecker/-/blame/573cbc3a5015b0a84a5fb5ce7f230c8d792d284a/cds_metadata/cds_variables_20190404.json
        # Change the variable inputs and dataset name
        'variables': ['skin_temperature'],
        'dataset_name': 'reanalysis-era5-single-levels',
        'product_type': 'reanalysis',
        'format': 'netcdf',
        'path': 'downloads'
    }

    cds = CDSAPIClient(config)
    asyncio.run(cds.launch())

