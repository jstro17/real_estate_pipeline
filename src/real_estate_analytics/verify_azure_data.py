import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AzureDataVerifier:
    def __init__(self, connection_string):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
    def list_blobs(self, container_name, prefix=None):
        """List all blobs in a container with optional prefix"""
        container_client = self.blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs(name_starts_with=prefix)
        return [blob.name for blob in blobs]
    
    def preview_blob_content(self, container_name, blob_name, max_lines=10):
        """Preview the content of a JSON blob"""
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        data = json.loads(blob_client.download_blob().readall())
        return json.dumps(data, indent=2)[:1000] + "..."  # Preview first 1000 chars
    
    def verify_state_data(self, container_name, state, year):
        """Verify data existence and structure for a specific state"""
        paths = [
            f"fair_market_rents/{year}/{state}/{state}_data.json",
            f"income_limits/{year}/{state}/{state}_data.json"
        ]
        
        for path in paths:
            try:
                logger.info(f"Verifying {path}...")
                preview = self.preview_blob_content(container_name, path)
                logger.info(f"Data preview:\n{preview}\n")
            except Exception as e:
                logger.error(f"Error accessing {path}: {e}")

if __name__ == "__main__":
    load_dotenv()
    
    AZURE_CONNECTION_STRING = os.getenv('AZURE_CONNECTION_STRING')
    if not AZURE_CONNECTION_STRING:
        raise ValueError("AZURE_CONNECTION_STRING not found in environment variables")
    
    verifier = AzureDataVerifier(AZURE_CONNECTION_STRING)
    container_name = "raw-data"
    
    # List all blobs in container
    logger.info("Listing all blobs in container:")
    blobs = verifier.list_blobs(container_name)
    for blob in blobs:
        logger.info(f"Found blob: {blob}")
    
    # Verify specific state data
    state = "CA"
    year = "2024"
    logger.info(f"\nVerifying data for {state} {year}:")
    verifier.verify_state_data(container_name, state, year)
