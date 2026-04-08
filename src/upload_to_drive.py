from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import google.auth.transport.requests
import os


def upload_csv_to_drive(file_path):
    SERVICE_ACCOUNT_FILE = "/opt/airflow/service_account.json"
    FOLDER_ID = "1G6awUxvySbM7WKRvdL5X_VMYCkozmSAv"
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    #  Validar archivo
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    print(f"📤 Subiendo archivo: {file_path}")

    try:
        # Credenciales
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=SCOPES
        )

        # FIX conexión (evita errores oauth / transporte)
        request = google.auth.transport.requests.Request()
        creds.refresh(request)

        # Cliente Drive (importante cache_discovery=False)
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
            resumable=False  # mas estable en Docker
        )

        # Subida con reintentos
        uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name",
        supportsAllDrives=True
).execute()

        print(f"Archivo subido correctamente: {uploaded_file['name']} (ID: {uploaded_file['id']})")

    except Exception as e:
        print(f"Error subiendo archivo a Drive: {str(e)}")
        raise