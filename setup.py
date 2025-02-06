from dotenv import load_dotenv
import os
from setuptools import setup, find_packages

load_dotenv()  # Loads .env file
api_key = os.getenv('RAPIDAPI_KEY')  # Access variables

subscription_id = os.getenv('SUBSCRIPTION_ID')

# Use subscription ID in Azure CLI command
cmd = f"az account set --subscription {subscription_id}"
subprocess.run(cmd, shell=True)

setup(
    name="real_estate_analytics",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        line.strip()
        for line in open("requirements.txt")
        if line.strip() and not line.startswith("#")
    ],
    author="Justin Castro",
    description="ETL pipeline for real estate analytics using HUD data",
    python_requires=">=3.8",
)