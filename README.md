# Climate Data Retrieval and Processing Script

## Overview
This script automates the process of downloading, extracting, and converting climate data from the Copernicus Climate Data Store (CDS). It retrieves climate variables such as 2m temperature and total precipitation, processes them into structured datasets, and converts them into formats like NetCDF, CSV, and GeoTIFF for further analysis and visualization.

## Features
- Download Climate Data: Retrieves reanalysis data from the Copernicus CDS API.
- Unzip Extracted Files: Automatically extracts downloaded .zip files.
- Convert NetCDF to DataFrame: Converts .nc files into pandas DataFrames.
- Aggregate Monthly Data: Converts daily climate records into monthly averages.
- Export to GeoTIFF: Saves processed climate data as GeoTIFF files for GIS applications.
- Logging: All processes and errors are logged in cds.log.

## Requirements
### Python Packages
Ensure you have the following installed before running the script:
pip install cdsapi xarray pandas numpy rasterio
If the lib.utils module is missing, ensure you have it or remove the related import.

### Copernicus CDS API Key
You need to register and configure Copernicus API credentials:
1. Sign up at: [Copernicus Climate Data Store](https://cds.climate.copernicus.eu/)
2. Obtain your API key and add it to ~/.cdsapirc:
      url = https://cds.climate.copernicus.eu/api/v2
   key = YOUR_USERNAME:YOUR_API_KEY
   verify = 0
   

## Usage
### Define the Countries and Variables
Modify the countries dictionary inside the script to include the desired locations:
countries: CountriesType = {
    "Ecuador": {
        "year": ["2008", "2009", "2010"],
        "min_lat": -6, "max_lat": 2,
        "min_lon": -82, "max_lon": -74
    }
}

### Run the Script
Execute the script using:
python CDS_auto_country.py

### Output Files
- Downloaded data: Stored as .zip files.
- Processed data: Converted into CSV, NetCDF, and GeoTIFF formats.
- Logs: Stored in cds.log.

## Functions
### download_cds_file()
Downloads climate data for a given country and year using the Copernicus CDS API.

### unzip_file()
Extracts the downloaded ZIP files containing NetCDF or GRIB data.

### convert_to_dataframe()
Converts NetCDF climate data into a structured pandas DataFrame.

### convert_to_monthly_nc()
Aggregates daily climate data into monthly averages and saves it in NetCDF format.

### create_tif_file()
Generates GeoTIFF files for climate data visualization in GIS applications.

## Example Workflow
The script downloads and processes climate data as follows:
1. Download ERA5 data from Copernicus CDS.
2. Extract ZIP files.
3. Convert .nc files to CSV for analysis.
4. Generate monthly NetCDF and GeoTIFF outputs.

## Logging & Debugging
- All logs and errors are recorded in cds.log.
- To enable debugging, modify the log level:
    logger.setLevel(logging.DEBUG)
  

## License
This script is open-source and available under the MIT License.
