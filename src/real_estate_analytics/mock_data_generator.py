import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
import random
from azure.storage.blob import BlobServiceClient
import json
import uuid
import os
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

def generate_crm_data(num_records=1000):
    data = []
    
    for _ in range(num_records):
        customer = {
            'customer_id': str(uuid.uuid4()),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state(),
            'zip_code': fake.zipcode(),
            'lead_source': random.choice(['Website', 'Referral', 'Real Estate Agent', 'Model Home']),
            'lead_status': random.choice(['New', 'Contacted', 'Qualified', 'Contract', 'Closed']),
            'created_date': fake.date_between(start_date='-2y').isoformat()
        }
        data.append(customer)
    
    return pd.DataFrame(data)

def generate_erp_data(num_records=500):
    data = []
    
    home_models = [
        {'model': 'Oakwood', 'base_price': 350000, 'sqft': 2200},
        {'model': 'Maple Grove', 'base_price': 425000, 'sqft': 2800},
        {'model': 'Pine Valley', 'base_price': 550000, 'sqft': 3400},
        {'model': 'Cedar Ridge', 'base_price': 650000, 'sqft': 3800}
    ]
    
    for _ in range(num_records):
        model = random.choice(home_models)
        start_date = fake.date_between(start_date='-1y')
        construction_days = random.randint(120, 180)
        
        project = {
            'project_id': str(uuid.uuid4()),
            'home_model': model['model'],
            'base_price': model['base_price'] + random.randint(-20000, 20000),
            'square_feet': model['sqft'],
            'lot_number': fake.building_number(),
            'subdivision': fake.city() + ' ' + random.choice(['Estates', 'Heights', 'Gardens', 'Commons']),
            'construction_start_date': start_date.isoformat(),
            'estimated_completion_date': (start_date + timedelta(days=construction_days)).isoformat(),
            'status': random.choice(['Planning', 'Foundation', 'Framing', 'Interior', 'Finishing', 'Complete']),
            'budget': model['base_price'] * 0.7,
            'actual_costs': model['base_price'] * 0.7 * random.uniform(0.95, 1.15)
        }
        data.append(project)
    
    return pd.DataFrame(data)

def upload_to_azure(container_name):
    """Upload data to Azure Blob Storage"""
    try:
        # Load environment variables
        load_dotenv()
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        
        if not connection_string:
            raise ValueError("AZURE_CONNECTION_STRING not found in environment variables")
        
        # Create the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Generate data
        crm_data = generate_crm_data()
        erp_data = generate_erp_data()
        
        # Get container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Upload CRM data
        logger.info("Uploading CRM data...")
        crm_blob_client = container_client.get_blob_client("crm_data.csv")
        crm_blob_client.upload_blob(crm_data.to_csv(index=False), overwrite=True)
        
        # Upload ERP data
        logger.info("Uploading ERP data...")
        erp_blob_client = container_client.get_blob_client("erp_data.csv")
        erp_blob_client.upload_blob(erp_data.to_csv(index=False), overwrite=True)
        
        logger.info("Upload completed successfully")
        
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        upload_to_azure("raw-data")
    except Exception as e:
        logger.error(f"Error: {e}")