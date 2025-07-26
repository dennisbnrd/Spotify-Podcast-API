import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from prefect_gcp import GcpCredentials
from prefect import flow

from extract import extract_podcast_data
from transform import transform_podcast_data
from load import upload_to_bigquery

# Load environment variables
load_dotenv()

# ==== Logging Setup ====
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"etl_log_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def run_pipeline(mode: str = "replace"):
    """
    Run the ETL pipeline for Spotify podcast data.
    mode: BigQuery if_exists mode ('replace', 'append', etc)
    """
    logging.info("=== STARTING ETL PIPELINE ===")

    # --- Extract ---
    logging.info("Step 1: Extracting podcast data ...")
    output_folder = "data"
    episodes_csv, meta_csv = extract_podcast_data(output_folder=output_folder)
    logging.info(f"Extract complete: {episodes_csv}, {meta_csv}")

    # --- Transform ---
    logging.info("Step 2: Transforming and cleaning data ...")
    clean_csv = os.path.join(output_folder, "podcast_episodes_clean.csv")
    transform_podcast_data(episodes_csv, clean_csv)
    logging.info(f"Transform complete: {clean_csv}")

    # --- Load ---
    logging.info("Step 3: Loading data to BigQuery ...")
    PROJECT_ID = os.getenv("PROJECT_ID") or "spotify-etl-466909"
    DATASET_TABLE = os.getenv("DATASET_TABLE") or "spotify_etl.podcast_episodes"
    SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE") or "config/spotify-etl-466909-9fc4d7e5e9b3.json"

    upload_to_bigquery(
        csv_file=clean_csv,
        project_id=PROJECT_ID,
        dataset_table=DATASET_TABLE,
        service_account_file=SERVICE_ACCOUNT_FILE,
        if_exists=mode
    )
    logging.info("=== PIPELINE COMPLETE ===")

if __name__ == "__main__":
    run_pipeline()
