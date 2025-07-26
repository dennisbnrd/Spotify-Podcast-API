import pandas as pd
from google.oauth2 import service_account
import pandas_gbq

def upload_to_bigquery(
    csv_file,
    project_id,
    dataset_table,
    service_account_file,
    if_exists='replace'
):
    """
    Upload file CSV ke BigQuery via pandas_gbq.
    """
    # Load data
    df = pd.read_csv(csv_file)
    print("Sample data:")
    print(df.head())

    # Credentials
    credentials = service_account.Credentials.from_service_account_file(service_account_file)

    # Upload
    print("Uploading data to BigQuery...")
    pandas_gbq.to_gbq(
        df,
        dataset_table,
        project_id=project_id,
        if_exists=if_exists,
        credentials=credentials
    )
    print("Upload selesai! Cek tabel di BigQuery.")

# =============================
# IMPLEMENTASI LANGSUNG (SAMPLE)
# =============================

if __name__ == "__main__":
    CSV_FILE = 'podcast_episodes_clean.csv'
    PROJECT_ID = 'spotify-etl-466909'
    DATASET_TABLE = 'spotify_etl.podcast_episodes'
    SERVICE_ACCOUNT_FILE = 'config\spotify-etl-466909-9fc4d7e5e9b3.json'

    upload_to_bigquery(
        csv_file=CSV_FILE,
        project_id=PROJECT_ID,
        dataset_table=DATASET_TABLE,
        service_account_file=SERVICE_ACCOUNT_FILE,
        if_exists='replace'   # atau 'append' kalau mau tambah data
    )
