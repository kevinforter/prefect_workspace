"""Upload von Dateien nach Google Drive aus Prefect.

Voraussetzung: Prefect Secret-Block `gdrive-token` mit folgendem JSON-Inhalt:
{
  "type": "authorized_user",
  "client_id": "...",
  "client_secret": "...",
  "refresh_token": "...",
  "token_uri": "https://oauth2.googleapis.com/token"
}
"""

import io
import json
from datetime import datetime, timezone

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

DEFAULT_FOLDER_ID = "1anLG5HmPHSO1jknvM-iNTbQMNeQvXp1B"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def _drive_service():
    value = Secret.load("gdrive-token").get()
    # Prefect liefert JSON-Secrets bereits als dict, Text-Secrets als str
    info = value if isinstance(value, dict) else json.loads(value)
    creds = Credentials.from_authorized_user_info(info, SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


@task(retries=2, retry_delay_seconds=30)
def upload_to_gdrive(
    data: bytes,
    name: str,
    mime_type: str = "application/octet-stream",
    folder_id: str = DEFAULT_FOLDER_ID,
    overwrite: bool = True,
) -> str:
    """Lädt Bytes als Datei in einen Drive-Ordner und gibt die Datei-ID zurück.

    overwrite=True: Existiert im Ordner bereits eine Datei mit gleichem Namen
    (die von dieser App erstellt wurde), wird ihr Inhalt ersetzt statt ein
    Duplikat anzulegen. Drive erlaubt sonst mehrere Dateien mit gleichem Namen.
    """
    logger = get_run_logger()
    service = _drive_service()
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=True)

    existing_id = None
    if overwrite:
        safe_name = name.replace("'", "\\'")
        query = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
        result = service.files().list(q=query, fields="files(id)", pageSize=1).execute()
        files = result.get("files", [])
        existing_id = files[0]["id"] if files else None

    if existing_id:
        file_id = service.files().update(
            fileId=existing_id, media_body=media, fields="id"
        ).execute()["id"]
        logger.info(f"Aktualisiert: {name} ({file_id}, {len(data)} Bytes)")
    else:
        file_id = service.files().create(
            body={"name": name, "parents": [folder_id]},
            media_body=media,
            fields="id",
        ).execute()["id"]
        logger.info(f"Hochgeladen: {name} ({file_id}, {len(data)} Bytes)")

    return file_id


@flow(name="gdrive-upload-test")
def gdrive_upload_test():
    """Erzeugt eine kleine CSV im Speicher und lädt sie nach Google Drive."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    csv = f"zeitpunkt,nachricht\n{now},hallo aus prefect\n".encode("utf-8")
    return upload_to_gdrive(csv, "prefect_test.csv", mime_type="text/csv")


if __name__ == "__main__":
    gdrive_upload_test()