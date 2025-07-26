# load.py
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
from prefect.blocks.system import Secret
import tempfile

def upload_to_bigquery(
    csv_file,
    project_id,
    dataset_table,
    service_account_block_name="bigquery-service-account",
    if_exists='replace'
):
    """
    Upload file CSV ke BigQuery via pandas_gbq,
    menggunakan Service Account dari Prefect Block Secret.
    """
    # Load service account JSON dari Prefect Secret block
    service_account_json = Secret.load(service_account_block_name).get()

    # Tulis ke file temp (dihapus otomatis saat program selesai)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmpfile:
        tmpfile.write(service_account_json)
        tmpfile.flush()
        temp_json_path = tmpfile.name

    print(f"[INFO] Credentials file created at {temp_json_path}")

    # Load data & upload
    df = pd.read_csv(csv_file)
    print("Sample data:")
    print(df.head())

    credentials = service_account.Credentials.from_service_account_file(temp_json_path)

    print("Uploading data to BigQuery...")
    pandas_gbq.to_gbq(
        df,
        dataset_table,
        project_id=project_id,
        if_exists=if_exists,
        credentials=credentials
    )
    print("Upload selesai! Cek tabel di BigQuery.")
