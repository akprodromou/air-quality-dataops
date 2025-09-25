# This script is the first step of this Air Quality Data Pipeline.
# It connects to the OpenAQ database and brings in data for the location specified (i.e. Thessaloniki)
# in raw (JSON) format. Prior to using it, one needs to have obtained an OPENAQ_API_KEY.

import requests
import json
import os
from datetime import datetime
# Import for environment variables
from dotenv import load_dotenv 

# Load environment variables from the .env file (in this case only the key),
# which is not pushed to remote. In production a secrets manager will be preferred.
load_dotenv()

# OpenAQ API v3 endpoints. This is where we get the data from
OPENAQ_LOCATIONS_API_URL = "https://api.openaq.org/v3/locations"

# City to fetch data for (default to Thessaloniki)
DEFAULT_CITY = os.getenv("OPENAQ_CITY", "Thessaloniki")

# Directory to store raw data files
# os.getenv checks if the RAW_DATA_PATH specified in docker-compose.yml exists (i.e. /opt/airflow/ingestion)
# if not, it uses the relative path provided
# RAW_DATA_PATH will now be relative to the container's root for the mounted data_ingestion folder
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH","./ingestion/raw_data")

# Get OpenAQ API Key from environment variables from load_dotenv()
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

# A function to fetch the location ID for a given city from the OpenAQ API v3
# The city name to be provided needs to be a string
def get_location_id(city_name: str) -> int | None:
    if not OPENAQ_API_KEY:
        print("Error: OPENAQ_API_KEY environment variable not set.")
        return None

    print(f"Searching for location ID for city: {city_name}...")
    # create a dictionary that will be sent along with the HTTP request to the OpenAQ API
    # X-API-Key is the naming convention required by OPENAQ
    headers = {"X-API-Key": OPENAQ_API_KEY}
    try:
        params = {
            "name": city_name,
            "limit": 1 # We only need one matching location
        }
        response = requests.get(OPENAQ_LOCATIONS_API_URL, params=params, headers=headers)
        # add a safety check to check for status code
        response.raise_for_status()
        # .json() parses the response into a Python object
        data = response.json()

        if data and data.get('results'):
            # get the first station for the specific city
            location = data['results'][0]
            location_id = location.get('locationsId')
            print(f"Found location ID {location_id} for {city_name}.")
            return location_id
        else:
            print(f"No location found for city: {city_name}.")
            return None
    # catch any HTTP request errors
    except requests.exceptions.RequestException as e:
        print(f"Error fetching location ID for {city_name}: {e}")
    # catch JSON response errors
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response for location ID: {e}")
    return None

# fetching the actual air quality measurements for a specific location ID
def fetch_air_quality_data(location_id: int) -> dict | None:
    if not OPENAQ_API_KEY:
        print("Error: OPENAQ_API_KEY environment variable not set.")
        return None

    api_url = f"{OPENAQ_LOCATIONS_API_URL}/{location_id}/latest"
    print(f"Attempting to fetch latest data for location ID: {location_id} from {api_url}...")
    headers = {"X-API-Key": OPENAQ_API_KEY}
    try:
        response = requests.get(api_url, headers=headers)
        # add a safety check to check for status code
        response.raise_for_status()
        # .json() parses the response into a Python object
        data = response.json()
        print(f"Successfully fetched data for location ID {location_id}.")
        return data
    # catch errors
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Response: {response.text}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An unexpected request error occurred: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"Error decoding JSON response: {json_err} - Response: {response.text}")
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

# --- Main Execution ---

if __name__ == "__main__":
    if not OPENAQ_API_KEY:
        print("\nERROR: OpenAQ API Key not found.")
        print("Please create a .env file in the same directory as this script with the following content:")
        print("OPENAQ_API_KEY='YOUR_ACTUAL_OPENAQ_API_KEY_HERE'")
        print("Alternatively, set the OPENAQ_API_KEY environment variable directly.")
    else:
        # Run the function we defined earlier to get the id for the specified city
        location_id = get_location_id(DEFAULT_CITY)
        if location_id:
            TARGET_LOCATION_ID = location_id 
            TARGET_LOCATION_NAME_FOR_FILENAME = DEFAULT_CITY.lower().replace(" ", "_")

            print(f"Attempting to fetch data for location ID: {TARGET_LOCATION_ID} ({TARGET_LOCATION_NAME_FOR_FILENAME})...")

            # Fetch data using the provided location ID
            air_quality_data = fetch_air_quality_data(TARGET_LOCATION_ID)

            # Save data if fetching was successful
            # Ensure 'results' array is not empty
            if air_quality_data and air_quality_data.get('results'): 
                save_raw_data(air_quality_data, TARGET_LOCATION_NAME_FOR_FILENAME, RAW_DATA_PATH)
                print("Data ingestion successful!")
            else:
                print(f"Failed to fetch air quality data or 'results' array was empty for location ID {TARGET_LOCATION_ID}.")
               
        else:
            print(f"Could not find a location ID for {DEFAULT_CITY}. No data fetched.")


