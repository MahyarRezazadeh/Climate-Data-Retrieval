import cdsapi
import xarray as xr
import pandas as pd
import numpy as np
import os
import glob
import logging
from typing import Dict, List, Union
import zipfile
import rasterio
from rasterio.transform import from_origin



logger = logging.getLogger('CDSLogger')
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('cds.log')
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

c = cdsapi.Client()

# This is a type variable to use for checking the types in mypy.
CountryType =  Dict[str,Union[List[str],int]]
CountriesType = Dict[str, CountryType]



def download_cds_file(country:CountryType,year:int, area:list, variable:List[str],format='netcdf') -> str:

    logger.info(f'start downloading data of {country} for year {year}')
    try:
        dataset = 'reanalysis-era5-single-levels'
        request = {
                'product_type': ['reanalysis'],
                'data_format': format,
                "download_format": "zip",
                'variable': variable,
                'year': [year],
                'month': [
                    '01', '02', '03',
                    '04', '05', '06',
                    '07', '08', '09',
                    '10', '11', '12',
                ],
                'day': [
                    '01', '02', '03',
                    '04', '05', '06',
                    '07', '08', '09',
                    '10', '11', '12',
                    '13', '14', '15',
                    '16', '17', '18',
                    '19', '20', '21',
                    '22', '23', '24',
                    '25', '26', '27',
                    '28', '29', '30',
                    '31',
                ],
                'time': [
                    '00:00', '01:00', '02:00',
                    '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00',
                    '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00',
                    '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00',
                    '21:00', '22:00', '23:00'
                    ],
                'area': area,
            }
        target = f'download_{country}_{year}.zip'
        c.retrieve(dataset,request,target)
        logger.info(f'finishing downloading data for {country} for year {year}')
        return target
       
    except Exception as e:
        logger.exception(f'error in {country} - {year}')


def unzip_file(zip_file_path,extract_to):
    if not os.path.exists(extract_to):
        os.mkdir(extract_to)
    try:
        with zipfile.ZipFile(zip_file_path,'r') as zf:
            zf.extractall(extract_to)
        logger.info('File Extracted Successfully')
    except Exception as e:
        logger.exception('error in unzip_file function in extracting zip file')

    
def convert_to_dataframe(country:str,year:int):
    logger.info(f'start converting downloaded file to dataframe')
    try:
        paths = glob.glob('unzip/*.nc')
        all_df = pd.DataFrame()
        for path in paths:
            ds = xr.open_dataset(path)
            ds = ds.rename_dims({'valid_time':'time'})
            ds = ds.rename_vars({'valid_time':'time'})
            ds = ds.drop_vars(['expver','number'])
            df = ds.to_dataframe()

            logger.info('finishing converting nc4 data to dataframe')

            df.reset_index('time',inplace=True)
            df['time'] = pd.to_datetime(df['time'])
            df['time'] = df['time'].dt.date
            df = df.groupby(['time','longitude','latitude']).mean()
            df.index = df.index.rename('lat',level='latitude')
            df.index = df.index.rename('lon',level='longitude')

            all_df = pd.concat([df,all_df],axis=1)
            df.index = df.index.set_levels(df.index.levels[0].astype(str),level='time')
            ds = xr.Dataset.from_dataframe(df)
            ds.attrs['CDI'] = 'Climate Data Interface version 2.0.1 (https://mpimet.mpg.de/cdi)'
            ds.attrs['Conventions'] = 'CF-1.6'
            ds.attrs['institution'] = 'European Centre for Medium-Range Weather Forecasts'
            ds.attrs['history'] = 'Tue Jun 21 21:31:10 2022: cdo -f nc copy /Users/Minoru/Downloads/2008_temp2.grib /Users/Minoru/Downloads/2008_temp.nc'
            ds.attrs['CDO'] = 'Climate Data Operators version 2.0.0 (https://mpimet.mpg.de/cdo)'
            variables_name = '_'.join(list(ds.data_vars.keys()))
            file_name = f'{country}/NC/{year}_{variables_name}.nc'
            directory_name = os.path.dirname(file_name)
            if not os.path.exists(directory_name):
                os.makedirs(directory_name)
                logger.info(f'Creating {directory_name} directories')
            ds.to_netcdf(file_name)
            os.remove(path)
            del ds,df

            logger.info(f'start writing dataframe to disk. The data will be available in the fowllowing address: CDS/{country}_{year}.csv')
        if not os.path.exists('CDS'):
            logger.info('Create CDS Direcotry')
            os.mkdir('CDS')
        all_df.to_csv(f'CDS/{country}_{year}.csv')
    except Exception as e:
        logger.exception('error occured in convert to dataframe')


def convert_to_monthly_nc(country:str,year:str,variable_name:str):
    try:
        logger.info(f'start converting for {country} on year {year} for variable_name:{variable_name}')
        input_path = f'{country}/NC/{year}_{variable_name}.nc'

        ds = xr.open_dataset(input_path)
        # Load the DataFrame from the dataset
        df = ds.to_dataframe().reset_index()

        # Ensure the 'time' column is a datetime object
        df['time'] = pd.to_datetime(df['time'])

        if variable_name == 't2m':
            df['t2m'] = df['t2m'] - 273.15

        # Create a 'year_month' column with the first day of each month
        df['time'] = df['time'].dt.to_period('M').dt.to_timestamp()

        # Group by 'year_month', 'lon', and 'lat' and compute the mean
        monthly_avg_df = df.groupby(['time', 'lon', 'lat'])[variable_name].mean().reset_index()

        monthly_avg_df.set_index(['time','lon','lat'],inplace=True)

        ds = xr.Dataset.from_dataframe(monthly_avg_df)
        ds.attrs['CDI'] = 'Climate Data Interface version 2.0.1 (https://mpimet.mpg.de/cdi)'
        ds.attrs['Conventions'] = 'CF-1.6'
        ds.attrs['institution'] = 'European Centre for Medium-Range Weather Forecasts'
        ds.attrs['history'] = 'Tue Jun 21 21:31:10 2022: cdo -f nc copy /Users/Minoru/Downloads/2008_temp2.grib /Users/Minoru/Downloads/2008_temp.nc'
        ds.attrs['CDO'] = 'Climate Data Operators version 2.0.0 (https://mpimet.mpg.de/cdo)'
        variables_name = '_'.join(list(ds.data_vars.keys()))
        file_name = f'{country}/nc/monthly/{year}_{variables_name}.nc'
        directory_name = os.path.dirname(file_name)
        if not os.path.exists(directory_name):
            os.makedirs(directory_name)
            logger.info(f"Create {directory_name} directories")
        ds.to_netcdf(file_name)
    except:
        logger.exception('error in the convert to monthly')


def create_tif_file(country:str,year:str,variable_name:str):
    ds = ds.sortby('lat',ascending=False)

    # Extract spatial dimensions
    lat = ds['lat'].values
    lon = ds['lon'].values

    # Calculate transform for GeoTIFF
    pixel_size_x = (lon[-1] - lon[0]) / (len(lon) - 1)
    pixel_size_y = (lat[-1] - lat[0]) / (len(lat) - 1)
    transform = from_origin(lon.min(), lat.max(), pixel_size_x, -pixel_size_y)

    # Output directory for the GeoTIFF files
    output_dir = f'{country}/TIF/{variable_name}/{year}'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


    # Loop through each time step (daily)
    for time_index, time_value in enumerate(ds['time'].values):
        # Extract data for the specific time step
        monthly_data = ds[variable_name].isel(time=time_index) # this code extract data in lon, lat 

        monthly_data = monthly_data.T # to convert data to lat,lon

        # Convert data to numpy array
        data_array = monthly_data.values

        # Ensure missing values (_FillValue) are handled
        data_array = np.nan_to_num(data_array, nan=-999.0)

        # Define output file path
        output_file = f"{output_dir}/{year}_{time_index + 1:02d}.tif"

        # Save as GeoTIFF
        with rasterio.open(
            output_file,
            "w",
            driver="GTiff",
            height=data_array.shape[0],
            width=data_array.shape[1],
            count=1,
            dtype=data_array.dtype,
            crs="+proj=latlong",
            transform=transform,
            nodata=-999.0,
        ) as dst:
            dst.write(data_array, 1)

        logger.info(f"Saved {output_file}")

if __name__ == '__main__':

    DOWNLOADED_FORMAT = ''

    variable = ['2m_temperature', 'total_precipitation']
    countries:CountriesType = {
    'Ecuador':{'year':['2008','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022','2023','2024','2025'],'min_lat':-6,'max_lat':2,'min_lon':-82,'max_lon':-74},
    }

    logger.info('All the errors and logs are located in the cds.log file.')
    if not os.path.exists('CDS'):
        os.makedirs('CDS')

    

    for country in countries.keys():
        logger.info(f'start downloading country {country}')
        years = countries[country]['year']
        if not isinstance(years,list):
            raise TypeError('Expected year to be a list')
        area = [
                countries[country]['max_lat'],countries[country]['min_lon'],countries[country]['min_lat'],
                countries[country]['max_lon'],
            ]
        for year in years:
            for variable_name in ['t2m','tp']:
                target_file_path = download_cds_file(country=country, year=year, area=area, variable=variable,format='grib')
                unzip_file(zip_file_path= target_file_path, extract_to='unzip')
                convert_to_dataframe(country=country,year=year)
                create_tif_file(country,year,variable_name)



