# This script is the first step of the Air Quality Data Pipeline. It's run inside air_quality_pipeline.py.
# It connects to the OpenAQ database and brings in data for the coordinates specified 
# in raw (JSON) format. Prior to using it, one needs to have obtained an OPENAQ_API_KEY.

from pathlib import Path
import requests
import json
import os
from datetime import datetime
# Import for environment variables
from dotenv import load_dotenv 

# Load environment variables from the .env file which is not pushed to remote. 
# The location coordinates and name should therefore be defined first in the .env file
# In production a secrets manager will be preferred.
load_dotenv()

# OpenAQ API v3 endpoints. This is where we get the data from
OPENAQ_LOCATIONS_API_URL = "https://api.openaq.org/v3/locations"

# Location to fetch data for 
CITY_NAME = os.getenv("CITY_NAME")
CITY_LAT = os.getenv("CITY_LAT")
CITY_LON = os.getenv("CITY_LON")

# Directory to store raw data files
# os.getenv checks if the RAW_DATA_PATH_AIR_QUALITY specified in docker-compose.yml exists (i.e. /opt/airflow/ingestion/air_quality)
# if not, it uses the relative path provided
RAW_DATA_PATH_AIR_QUALITY = Path(os.getenv("RAW_DATA_PATH_AIR_QUALITY","./ingestion/raw_data/air_quality"))
RAW_DATA_PATH_AIR_QUALITY.mkdir(parents=True, exist_ok=True)

# Get OpenAQ API Key from environment variables from load_dotenv()
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

# A function to fetch the station name for a given city from the OpenAQ API v3
def get_station_data(city_lat: int, city_lon: int) -> int | None:
    if not OPENAQ_API_KEY:
        print("Error: OPENAQ_API_KEY environment variable not set.")
        return None

    print(f"Searching for a sensor for station: {city_lat},{city_lon}...")
    # create a dictionary that will be sent along with the HTTP request to the OpenAQ API
    # X-API-Key is the naming convention required by OPENAQ
    headers = {"X-API-Key": OPENAQ_API_KEY}

    try:
        params = {
            "coordinates": f"{city_lat},{city_lon}",
            "radius": 5000, 
            "limit": 1000
        }
        response = requests.get(OPENAQ_LOCATIONS_API_URL, params=params, headers=headers)
        print(response)
        # add a safety check to check for status code
        response.raise_for_status()
        # .json() parses the response into a Python object
        data = response.json()
        # print(json.dumps(data, indent=4))

        if data and data.get('results'):
            # get the first station for the specific city
            station = data['results'][0]
            station_name = station.get('name')
            station_id = station.get('id')
            print(f"Found station named {station_name} "
                  f"for coordinates {city_lat},{city_lon} "
                  f"with id = {station_id}.")
            return data
        else:
            print(f"No station found for coordinates {city_lat},{city_lon}.")
            return None
    # catch any HTTP request errors
    except requests.exceptions.RequestException as e:
        print(f"Error fetching station name for coordinates {city_lat},{city_lon}: {e}")
    # catch JSON response errors
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response for station name: {e}")
    return None

# now take the data from the previous step and save it to the directory
def save_raw_data(data: dict, city: str, directory: str):
    # Create the directory if it doesn't exist
    os.makedirs(directory, exist_ok=True)

    # Generate a timestamped filename for uniqueness and traceability
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"openaq_data_{city.lower().replace(' ', '_')}_{timestamp}.json"
    # os.path.join automatically adds the correct separator (\ or /) between the parts of the path
    filepath = os.path.join(directory, filename)

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"Raw data saved successfully to: {filepath}")
    except IOError as io_err:
        print(f"Error saving file {filepath}: {io_err}")
    except Exception as e:
        print(f"An unexpected error occurred while saving data: {e}")

# Main Execution
# Uses the two functions defined above

# runs this code only when running as a script, not at import
if __name__ == "__main__":
    if not OPENAQ_API_KEY:
        print("\nERROR: OpenAQ API Key not found.")
        print("Obtain a key and create a .env file in the same directory as this script with the following content:")
        print("OPENAQ_API_KEY='YOUR_ACTUAL_OPENAQ_API_KEY_HERE'")
    else:
        # Run the function we defined earlier to get the data for the specified city
        station_data = get_station_data(CITY_LAT, CITY_LON)
        if station_data:
            # Fetch data using the provided station name
            air_quality_data = station_data
            # Save data if fetching was successful
            # Ensure 'results' array is not empty
            if air_quality_data and air_quality_data.get('results'): 
                save_raw_data(air_quality_data, CITY_NAME, RAW_DATA_PATH_AIR_QUALITY)
                print("Data ingestion successful!")
            else:
                print(f"Failed to fetch air quality data or 'results' array was empty for city {CITY_NAME}.")
               
        else:
            print(f"Could not find an AQ monitoring station for {CITY_LAT, CITY_LON}. No data fetched.")


