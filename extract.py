import requests
from dotenv import load_dotenv
import os 
import logging

# Load environment variables from .env file
load_dotenv(dotenv_path='/NYC_ETL/config.env')


# Configure logging
logging.basicConfig(
    filename = '/NYC_ETL/logs/extract.log',
    level = logging.INFO, 
    format='%(asctime)s - %(levelname)s -%(message)s'
)

# Extract function to download NYC Taxi data
# This function downloads the NYC Taxi data for a given year and month from the specified URL
logging.info("Starting the data extraction process for NYC Taxi data.")
def download_nyc_taxi_data(url, path, year, month):

    try:
        year_str = str(year)
        month_str = f"{month:02d}"  # Format month as two digits (e.g., '01', '02', ..., '12')
        folder_path = os.path.join(path, year_str, month_str)
        os.makedirs(folder_path, exist_ok=True)

        file_name = f"yellow_tripdata_{year_str}-{month_str}.parquet"
        file_path = os.path.join(folder_path, file_name)

        # Check if the file already exists
        if os.path.exists(file_path):
            logging.info(f"File {file_name} already exists, skipping download.")
            return

        # Send a GET request to the URL 
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        response.raise_for_status()

        # Write the content of the response to the specified file path
        with open(file_path, 'wb') as file:
            file.write(response.content)
        
        file_size = os.path.getsize(file_path)
        logging.info(f"successfully downloaded: {file_path}  ({file_size}bytes)")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download data from URL: {url}. Error: {str(e)}")



path = os.getenv("RAW_LOCATION")  
if not path:
    logging.error("RAW_LOCATION environment variable is not set.")
    exit(1)


for year in range(2024, 2025):
    for month in range(1, 13):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
        logging.info(f"Downloading data from {url} for year: {year} and month: {month:02d}")
        download_nyc_taxi_data(url, path, year, month)

logging.info("Data extraction process completed successfully.")


# want to change the year 


