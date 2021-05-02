import os
from google.cloud import bigquery
from google.oauth2 import service_account


key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
scope = "https://www.googleapis.com/auth/cloud-platform"


def auth() -> (service_account.Credentials, bigquery.Client):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=[scope],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    return credentials, client

