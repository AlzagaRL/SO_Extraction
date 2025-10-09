import glob
import json
import cdsapi
import xarray as xr
from typing import Dict
from pathlib import Path
from datetime import datetime, timedelta


class CDSAPI:
    def __init__(self, config: Dict):
        self.days = None
        self.months = None
        self.years = None
        self.hours = None
        self.config = config
        self.path = config['path']
        self.start = config['date_control']['start']
        self.end = config['date_control']['today']
        self.latest = config['date_control']['today'] - timedelta(days=self.config['date_control']['delay'])
        Path(self.path).mkdir(exist_ok=True)

    def scope_years(self):
        end = self.latest.replace(hour=0, minute=0, second=0, microsecond=0)
        date_list = [self.start + timedelta(days=x) for x in range((end - self.start).days + 1)]
        self.years = sorted(list(set([d.strftime('%Y') for d in date_list])))

    def scope_date(self, year: int):
        # if str(self.end.year) == year:
        #     end = self.latest.replace(hour=0, minute=0, second=0, microsecond=0)
        #     start = datetime(int(year), 1, 1)
        #     date_list = [start + timedelta(days=x) for x in range((end - start).days + 1)]
        if str(self.latest.year) == year:
            start = datetime(int(year), 1, 1)
            if start < self.start:
                start = self.start
            end = self.latest.replace(hour=0, minute=0, second=0, microsecond=0)
            date_list = [start + timedelta(days=x) for x in range((end - start).days + 1)]
        else:
            end = datetime(int(year), 12, 31)
            date_list = [self.start + timedelta(days=x) for x in range((end - self.start).days + 1)]
            
        self.days = sorted(list(set([d.strftime('%d') for d in date_list])))
        self.months = sorted(list(set([d.strftime('%m') for d in date_list])))
        self.hours = [str(h).zfill(2) for h in range(0, 24)]

    def duration(self):
        start = self.config['date_control']['start']
        end = self.config['date_control']['today']
        latest = end - timedelta(days=self.config['date_control']['delay'])
        end = latest.replace(hour=0, minute=0, second=0, microsecond=0)

        date_list = [start + timedelta(days=x) for x in range((end - start).days + 1)]
        self.days = sorted(list(set([d.strftime('%d') for d in date_list])))
        self.months = sorted(list(set([d.strftime('%m') for d in date_list])))
        self.years = sorted(list(set([d.strftime('%Y') for d in date_list])))
        self.hours = [str(h).zfill(2) for h in range(0, 24)]

    def check(self):
        print(self.days)
        print(self.months)
        print(self.years)
        print(self.hours)

    def call(self):
        client = cdsapi.Client()
        for variable in config['variables']:
            Path(f"{self.path}/{variable}").mkdir(exist_ok=True)
            for year in self.years:
                # print(f"FOR YEAR {year}")
                self.scope_date(year)
                #self.check()
                for month in self.months:
                    client.retrieve(
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

    def convert_daily(self):
        # ds = xr.open_dataset(Path(self.path) / config['variable'][0] / f"{self.years[0]}-{self.months[0]}.nc")
        # ds = ds.rename({'valid_time': 'time'})

        # daily_mean = ds.resample(time='1D').mean()
        # daily_mean.to_netcdf(Path(self.path) / config['filename_aggregations']['mean'])

        # daily_min = ds.resample(time='1D').min()
        # daily_min.to_netcdf(Path(self.path) / config['filename_aggregations']['minimum'])

        # daily_max = ds.resample(time='1D').max()
        # daily_max.to_netcdf(Path(self.path) / config['filename_aggregations']['maximum'])
        entire_dataset = []
        regional_coords = self.load_regions_coords()
        for year in self.years:
            file_pattern = Path(self.path) / self.config['variables'][0] / f'{year}-*.nc'
            file_paths = glob.glob(str(file_pattern))
            print(file_pattern)
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
                
                # for region_data in regional_coords:
                #     xe = daily_aggregated.interp(latitude=region_data['Latitude'], longitude=region_data['Longitude'])
                #     xe = xe.assign(loc_name=region_data['Location'])
                # print(xe.to_dataframe().reset_index())

                #print(daily_aggregated)
                #daily_aggregated = daily_aggregated.interp(latitude=13.216667, longitude=123.55)
                #print(daily_aggregated.to_dataframe().reset_index())
                monthly_aggregated.append(daily_aggregated)
            #print(monthly_aggregated.to_dataframe().reset_index())
            #print(monthly_aggregated)
            yearly_aggregated = xr.concat(monthly_aggregated, dim='valid_time')
            print(f"{year} success:", yearly_aggregated)
            print(yearly_aggregated.to_dataframe().reset_index())
            #print(yearly_aggregated)
            entire_dataset.append(yearly_aggregated)

        complete_dataset = xr.concat(entire_dataset, dim='valid_time')
        # Test
        complete_dataset = complete_dataset.interp(latitude=13.216667, longitude=123.55)
        print(complete_dataset.to_dataframe().reset_index())

  
    def load_province_coords(self):
        import json
        with open('utils/ph_province_coordinates.json', 'r', encoding='utf-8') as f:
            province_coordinates = json.load(f)
        return province_coordinates
    

    def concat_all(self):
        print("CONCAT all files *.nc in the variable/indicator directory")

        xr.set_options(use_new_combine_kwarg_defaults=True)
        with open('utils/ph_province_coordinates.json', 'r', encoding='utf-8') as f:
            provinces = json.load(f)
        
        for var in self.config['variables']:
            file_pattern = Path(self.path) / var / '*.nc'
            file_paths = glob.glob(str(file_pattern))
            dataset = []

            for file_path in file_paths:
                try:
                    file = xr.open_dataset(file_path)
                    print(file_path)
                    dataset.append(file)
                except Exception as e:
                    print("Empty, skipping.")
                    continue

            complete_dataset =  xr.concat(dataset, dim='valid_time')
            daily_means = complete_dataset.resample(valid_time='1D').mean()
            daily_minimums = complete_dataset.resample(valid_time='1D').min()
            daily_maximums = complete_dataset.resample(valid_time='1D').max()

            complete_dataset_daily_aggregated = xr.Dataset({
                "daily_mean": daily_means.to_array(),
                "daily_min": daily_minimums.to_array(),
                "daily_max": daily_maximums.to_array()
            })

            provinces_records = []
            for province in provinces:
                province_record = complete_dataset_daily_aggregated.interp(latitude=province['Latitude'], longitude=province['Longitude'], method="linear")
                province_record = province_record.assign_coords(location_name=province['Location'])
                provinces_records.append(province_record)

            all_provinces_record = xr.concat(provinces_records, dim='location_name')
            all_provinces_record.to_netcdf(f'{var}.nc')
            all_provinces_record.to_dataframe().reset_index().to_csv(f'{var}.csv')


        #print(all_provinces_record.to_dataframe().reset_index())
        # xre = all_provinces_record.to_dataframe().reset_index()
        # print(xre[xre['location_name'] == "Negros Occidental, Negros Island Region, Philippines"])
        # print(xre[xre['location_name'] == "Negros Oriental, Negros Island Region, Philippines"])
        # print(xre)
        # xre.to_csv("provinces_data.csv")

            

    # def load(self, lat: float, lon: float):
    #     ds = xr.open_dataset(Path(self.path) / '2m_temperature' /  '1981-07.nc')

    #     # Estimation
    #     # ds = ds.sel(latitude=lat, longitude=lon, method="nearest")

    #     # High Precision
    #     #ds = ds.interp(latitude=lat, longitude=lon)

    #     df = ds.to_dataframe().reset_index()
    #     print(df)

    def main(self):
        self.scope_years()
        #self.concat_all()
        self.call()
        #self.load_regions_coords()
        #self.convert_daily()
        self.load(13.216667, 123.55)
        #self.check()


config = {
    'date_control' : {
        # 'start': datetime(1980, 6, 27),
        # #'today': datetime.now(),
        # 'today': datetime(1982, 1, 2),
        'start': datetime(1981, 1, 1),
        'today': datetime.now(),
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
        "south":5.58100332277,
        "east": 126.537423944,
        "north": 18.5052273625
    },
    #'variables': ['2m_temperature', 'total_precipitation'],
    'variables': ['surface_pressure'],
    'product_type': 'reanalysis',
    'base': 'reanalysis-era5-single-levels',
    'format': 'netcdf',
    'filename': '2m_temperature.nc',
    'path': 'downloads'
}
cds = CDSAPI(config)
cds.main()