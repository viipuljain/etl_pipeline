# api_to_s3

This is a Dagster pipeline that extracts data from the Hacker News API, transforms it, and writes it to an S3 bucket in Parquet format.

## Project Structure

- `api_to_s3/__init__.py` – Defines the pipeline and schedule.  
- `api_to_s3/parquet_io_manager.py` – Custom IO manager for handling Parquet file storage.  
- `api_to_s3/assets/hackernews.py` – Contains the asset for fetching and processing Hacker News data.  

