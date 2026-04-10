import os

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


def upload_csv_to_drive(file_path):
    TOKEN_FILE = "/opt/airflow/token.json"
    FOLDER_ID = "1G6awUxvySbM7WKRvdL5X_VMYCkozmSAv"
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    if not os.path.exists(TOKEN_FILE):
        raise FileNotFoundError(f"No se encontró el token OAuth: {TOKEN_FILE}")

    print(f"Subiendo archivo: {file_path}")

    try:
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_FILE, "w") as token:
                token.write(creds.to_json())

        service = build(
            "drive",
            "v3",
            credentials=creds,
            cache_discovery=False
        )

        file_metadata = {
            "name": os.path.basename(file_path),
            "parents": [FOLDER_ID]
        }

        media = MediaFileUpload(
            file_path,
            mimetype="text/csv",
            resumable=False
        )

        uploaded_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, name"
        ).execute(num_retries=3)

        print(
            f"Archivo subido correctamente: "
            f"{uploaded_file['name']} (ID: {uploaded_file['id']})"
        )

    except Exception as e:
        print(f"Error subiendo archivo a Drive: {str(e)}")
        raise