# Real Estate Analytics Pipeline

ETL pipeline for HUD data and real estate analytics, integrating with Azure Data Factory and Snowflake.

## Components

- HUD API data extraction
- Mock CRM/ERP data generation
- Azure Blob Storage integration
- Snowflake data warehouse integration
- Azure Data Factory pipelines

## Setup

1. Clone the repository
2. Create a `.env` file with required credentials:
```env
HUD_API_TOKEN=your_token
AZURE_CONNECTION_STRING=your_connection_string
```
3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the HUD pipeline:
```bash
python hud_pipeline.py
```

Generate mock data:
```bash
python mock_data_generator.py
```

Verify data in Azure:
```bash
python verify_azure_data.py
```
