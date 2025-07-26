import pandas as pd
from pandas_gbq import to_gbq
import logging
from prefect_gcp import GcpCredentials

logging.basicConfig(level=logging.INFO)

def upload_to_bigquery(
    csv_file,
    project_id,
    dataset_table,
    gcp_credentials_block_name="gcp-credentials",
    if_exists='replace'
):
    """
    Upload file CSV ke BigQuery via pandas_gbq,
    menggunakan Service Account dari Prefect GcpCredentials Block.
    """
    try:
        # Load GCP Credentials dari Prefect Block GcpCredentials
        credentials = GcpCredentials.load(gcp_credentials_block_name).get_credentials_from_service_account()

        # Load data
        df = pd.read_csv(csv_file)
        logging.info(f"Sample data:\n{df.head()}")

        # Upload ke BigQuery
        logging.info(f"Uploading to BigQuery table: {dataset_table} (mode: {if_exists})")
        to_gbq(
            dataframe=df,
            destination_table=dataset_table,
            project_id=project_id,
            if_exists=if_exists,
            credentials=credentials
        )
        logging.info("Upload selesai! Cek tabel di BigQuery.")
    except Exception as e:
        logging.error(f"Failed to upload to BigQuery: {e}")
