from google.oauth2 import service_account
from google.cloud import bigquery


def create_bq_connector(key_path):
    # Construct a BigQuery client object.
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    return client


def create_bq_connector_full_access():
    # Construct a BigQuery client object.
    client = bigquery.Client()
    return client
