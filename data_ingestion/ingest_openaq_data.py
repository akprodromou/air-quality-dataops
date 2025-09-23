import requests
import json
import os
from datetime import datetime
# New import for environment variables
from dotenv import load_dotenv 

# Load environment variables from the .env file
load_dotenv()

# --- Configuration ---
# OpenAQ API v3 endpoints
OPENAQ_LOCATIONS_API_URL = "https://api.openaq.org/v3/locations"

# Default city to fetch data for. 
DEFAULT_CITY = "Thessaloniki"
# Directory to store raw data files
# os.path.join automatically adds the correct separator (\ or /) between the parts of the path
# RAW_DATA_DIR will now be relative to the container's root for the mounted data_ingestion folder
# The data_ingestion folder is mounted to /opt/airflow/data_ingestion as per docker-compose.yml
# So raw_data will be /opt/airflow/data_ingestion/raw_data
RAW_DATA_DIR = "./data_ingestion/raw_data"

# Get OpenAQ API Key from environment variables from load_dotenv()
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

# --- Helper Functions ---

def get_location_id(city_name: str) -> int | None:
    """
    Fetches the location ID for a given city from the OpenAQ API v3.

    Args:
        city_name (str): The name of the city to find the location ID for.

    Returns:
        int | None: The location ID if found, otherwise None.
    """
    if not OPENAQ_API_KEY:
        print("Error: OPENAQ_API_KEY environment variable not set.")
        return None

    print(f"Searching for location ID for city: {city_name}...")
    headers = {"X-API-Key": OPENAQ_API_KEY}
    try:
        params = {
            "name": city_name,
            "limit": 1 # We only need one matching location
        }
        response = requests.get(OPENAQ_LOCATIONS_API_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        if data and data.get('results'):
            location = data['results'][0]
            location_id = location.get('id')
            print(f"Found location ID {location_id} for {city_name}.")
            return location_id
        else:
            print(f"No location found for city: {city_name}.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching location ID for {city_name}: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response for location ID: {e}")
    return None

def fetch_air_quality_data(location_id: int) -> dict | None:
    """
    Fetches the latest air quality data for a given location ID from the OpenAQ API v3.

    Args:
        location_id (int): The unique ID of the location to fetch data for.

    Returns:
        dict | None: A dictionary containing the API response data if successful,
                     otherwise None.
    """
    if not OPENAQ_API_KEY:
        print("Error: OPENAQ_API_KEY environment variable not set.")
        return None

    api_url = f"{OPENAQ_LOCATIONS_API_URL}/{location_id}/latest"
    print(f"Attempting to fetch latest data for location ID: {location_id} from {api_url}...")
    headers = {"X-API-Key": OPENAQ_API_KEY}
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        print(f"Successfully fetched data for location ID {location_id}.")
        return data
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

def save_raw_data(data: dict, city: str, directory: str):
    """
    Saves the raw air quality data to a JSON file in the specified directory.

    Args:
        data (dict): The data to save.
        city (str): The city associated with the data (used in filename).
        directory (str): The directory where the file will be saved.
    """
    # Create the directory if it doesn't exist
    os.makedirs(directory, exist_ok=True)

    # Generate a timestamped filename for uniqueness and traceability
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"openaq_data_{city.lower().replace(' ', '_')}_{timestamp}.json"
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
        # We will directly use the location ID you found.
        # This ID corresponds to the "AGIA SOFIA" station in Thessaloniki, Greece.
        TARGET_LOCATION_ID = 2162785 # <--- Use the exact ID you found
        TARGET_LOCATION_NAME_FOR_FILENAME = "thessaloniki_agia_sofia" # A descriptive name for the filename

        print(f"Attempting to fetch data for location ID: {TARGET_LOCATION_ID} ({TARGET_LOCATION_NAME_FOR_FILENAME})...")

        # Fetch data using the provided location ID
        air_quality_data = fetch_air_quality_data(TARGET_LOCATION_ID)

        # Save data if fetching was successful
        if air_quality_data and air_quality_data.get('results'): # Ensure 'results' array is not empty
            save_raw_data(air_quality_data, TARGET_LOCATION_NAME_FOR_FILENAME, RAW_DATA_DIR)
            print("Data ingestion successful!")
        else:
            print(f"Failed to fetch air quality data or 'results' array was empty for location ID {TARGET_LOCATION_ID}.")
            print("No data saved. This might mean no current data is available for this station.")


