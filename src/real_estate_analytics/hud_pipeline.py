import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import json
import os
from dotenv import load_dotenv
import logging
import time
from tenacity import retry, wait_exponential, stop_after_attempt
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import json as pa_json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HUDDataPipeline:
    def __init__(self, token):
        self.headers = {
            "Authorization": f"Bearer {token}"
        }
        self.base_url = "https://www.huduser.gov/hudapi/public"
        # Initialize without connection string until explicitly provided
        self.blob_service_client = None
        self.request_delay = 1.0  # Delay between requests in seconds
        
    def set_azure_connection(self, connection_string):
        """Set Azure connection after initialization"""
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
    def get_states(self):
        """Get list of all states from HUD API"""
        url = f"{self.base_url}/fmr/listStates"
        return self._make_request(url)

    def get_counties(self, state_code):
        """Fetch counties for a given state code."""
        url = f"{self.base_url}/fmr/listCounties/{state_code}"  # Updated URL format
        return self._make_request(url)

    def get_metro_areas(self):
        """Get list of all metropolitan areas from HUD API"""
        url = f"{self.base_url}/fmr/listMetroAreas"
        return self._make_request(url)

    def _make_request(self, url, params=None):
        """Make API request with error handling and rate limiting"""
        @retry(
            wait=wait_exponential(multiplier=1, min=4, max=10),
            stop=stop_after_attempt(3)
        )
        def _execute_request():
            time.sleep(self.request_delay)  # Rate limiting
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                json_response = response.json()
                
                if isinstance(json_response, dict) and "data" in json_response:
                    return json_response["data"]
                elif isinstance(json_response, list):
                    return json_response
                else:
                    logger.error(f"Unexpected response structure: {json_response}")
                    raise Exception("Unexpected API response structure")
            elif response.status_code == 429:
                logger.warning("Rate limit hit, backing off...")
                raise Exception("Rate limit exceeded")
            else:
                logger.error(f"Error response: {response.text}")
                raise Exception(f"API request failed with status {response.status_code}")

        try:
            return _execute_request()
        except Exception as e:
            logger.error(f"Request failed after retries: {e}")
            raise

    def get_fair_market_rents(self, year=None, state=None, entity_id=None):
        """Fetch Fair Market Rents data from HUD API"""
        if entity_id:
            url = f"{self.base_url}/fmr/data/{entity_id}"
        elif state:
            url = f"{self.base_url}/fmr/statedata/{state}"
        else:
            raise ValueError("Either state code or entity_id must be provided")

        params = {"year": year} if year else None
        return self._make_request(url, params)

    def get_income_limits(self, year=None, state=None, entity_id=None):
        """Fetch Income Limits data from HUD API"""
        if entity_id:
            url = f"{self.base_url}/il/data/{entity_id}"
        elif state:
            url = f"{self.base_url}/il/statedata/{state}"
        else:
            raise ValueError("Either state code or entity_id must be provided")

        params = {"year": year} if year else None
        return self._make_request(url, params)

    def get_county_data(self, state_code, year):
        """Fetch detailed data for all counties in a state"""
        counties = self.get_counties(state_code)
        county_data = []
        
        for county in counties:
            try:
                fips_code = county['fips_code']
                logger.info(f"Processing {county['county_name']}")
                
                # Fetch both FMR and IL data for each county with longer delay
                time.sleep(2)  # Additional delay for county-level requests
                fmr = self.get_fair_market_rents(year=year, entity_id=fips_code)
                time.sleep(2)  # Delay between FMR and IL requests
                il = self.get_income_limits(year=year, entity_id=fips_code)
                
                county_data.append({
                    'county_info': county,
                    'fair_market_rents': fmr,
                    'income_limits': il
                })
                logger.info(f"Successfully processed {county['county_name']}")
            except Exception as e:
                logger.error(f"Error processing county {county['county_name']}: {e}")
                continue  # Continue with next county even if one fails
        
        return county_data

    def save_to_azure(self, data, dataset_name, year, state):
        """
        Save data to Azure Blob Storage with structure:
        container/dataset_name/year/state/state_data.json
        """
        if not self.blob_service_client:
            raise ValueError("Azure connection not set. Call set_azure_connection first.")
            
        try:
            # Get container client
            container_client = self.blob_service_client.get_container_client("raw-data")
            
            # Create blob path
            blob_path = f"{dataset_name}/{year}/{state}/{state}_data.json"
            
            # Upload data
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(json.dumps(data, indent=2), overwrite=True)
            
            logger.info(f"Saved to Azure: {blob_path}")
            
        except Exception as e:
            logger.error(f"Failed to save to Azure: {e}")
            raise

    def save_to_azure_parquet(self, data, dataset_name, year, state):
        """Save data as Parquet in Azure"""
        if not self.blob_service_client:
            raise ValueError("Azure connection not set")
            
        try:
            # Convert JSON to DataFrame then to Parquet
            # First convert to JSON string
            json_data = json.dumps(data)
            
            # Parse JSON to PyArrow Table
            table = pa_json.read_json(
                pa.py_buffer(json_data.encode()),
                parse_options=pa_json.ParseOptions(explicit_schema=None)
            )
            
            # Write to memory buffer
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer)
            
            # Get container client
            container_client = self.blob_service_client.get_container_client("raw-data")
            
            # Create blob path
            blob_path = f"{dataset_name}/{year}/{state}/{state}_data.parquet"
            
            # Upload parquet file
            blob_client = container_client.get_blob_client(blob_path)
            blob_client.upload_blob(buffer.getvalue().to_pybytes(), overwrite=True)
            
            logger.info(f"Saved Parquet to Azure: {blob_path}")
            
        except Exception as e:
            logger.error(f"Failed to save Parquet to Azure: {e}")
            raise

    def process_state(self, state_code, year, storage_type="azure"):
        """Process all data for a single state"""
        logger.info(f"Processing {state_code} for year {year}")
        try:
            # Get state-level data
            fmr_data = self.get_fair_market_rents(year=year, state=state_code)
            il_data = self.get_income_limits(year=year, state=state_code)
            
            # Get county-level data
            county_data = self.get_county_data(state_code, year)
            
            # Prepare data packages
            fmr_package = {
                'state_level': fmr_data,
                'counties': county_data
            }
            
            il_package = {
                'state_level': il_data,
                'counties': county_data
            }
            
            # Save based on storage type
            if storage_type == "local" or storage_type == "both":
                self.save_locally(fmr_package, "fair_market_rents", year, state_code)
                self.save_locally(il_package, "income_limits", year, state_code)
                
            if storage_type == "azure" or storage_type == "both":
                self.save_to_azure(fmr_package, "fair_market_rents", year, state_code)
                self.save_to_azure(il_package, "income_limits", year, state_code)
                
                # Save as Parquet
                self.save_to_azure_parquet(fmr_package, "fair_market_rents_parquet", year, state_code)
                self.save_to_azure_parquet(il_package, "income_limits_parquet", year, state_code)
            
            logger.info(f"Successfully processed {state_code}")
            
        except Exception as e:
            logger.error(f"Error processing {state_code}: {e}")
            raise

    def save_locally(self, data, dataset_name, year, state):
        """
        Save data locally as JSON with structure:
        data/{dataset_name}/{year}/{state}/{state}_data.json
        """
        if not data:
            raise ValueError(f"No data provided for {dataset_name}")
            
        # Create directory structure
        directory = f"data/{dataset_name}/{year}/{state}"
        os.makedirs(directory, exist_ok=True)
        
        # Save data
        filepath = f"{directory}/{state}_data.json"
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {filepath} locally")

# Example usage
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Get credentials from environment variables
    TOKEN = os.getenv('HUD_API_TOKEN')
    AZURE_CONNECTION_STRING = os.getenv('AZURE_CONNECTION_STRING')
    
    if not TOKEN or not AZURE_CONNECTION_STRING:
        raise ValueError("Missing required environment variables")
    
    # Initialize pipeline
    pipeline = HUDDataPipeline(TOKEN)
    pipeline.set_azure_connection(AZURE_CONNECTION_STRING)
    
    # Configuration
    STATES_TO_PROCESS = ['TX', 'FL', 'CO', 'AZ', 'CA']
    YEAR = 2024
    STORAGE_TYPE = "azure"  # Change this value to:
                           # "azure" - store in Azure only
                           # "local" - store locally only
                           # "both"  - store in both Azure and locally
    
    try:
        for state in STATES_TO_PROCESS:
            pipeline.process_state(state, YEAR, STORAGE_TYPE)
        logger.info("Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise