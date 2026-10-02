"""
Einmaliges Google-Drive-OAuth-Setup für meteoswiss_to_gdrive.py

1. Google Cloud Console -> neues Projekt -> "Google Drive API" aktivieren
2. OAuth-Zustimmungsbildschirm: Typ "Extern", dich selbst als Testnutzer eintragen,
   danach auf "In Produktion" stellen (sonst läuft das Refresh-Token nach 7 Tagen ab)
3. Anmeldedaten -> OAuth-Client-ID -> Typ "Desktop-App" -> JSON herunterladen
4. python gdrive_auth_setup.py --client-secret ~/Downloads/client_secret_xxx.json

Das Skript öffnet den Browser, du meldest dich an, und das Token wird als
Prefect Secret Block "gdrive-oauth-token" gespeichert.

Scope ist drive.file: Der Flow sieht nur Dateien, die er selbst angelegt hat,
nicht dein restliches Drive.
"""

from __future__ import annotations

import argparse

from google_auth_oauthlib.flow import InstalledAppFlow
from prefect.blocks.system import Secret

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
TOKEN_BLOCK = "gdrive-oauth-token"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--client-secret", required=True, help="Pfad zur OAuth-Client-JSON")
    args = p.parse_args()

    oauth = InstalledAppFlow.from_client_secrets_file(args.client_secret, SCOPES)
    creds = oauth.run_local_server(port=0, access_type="offline", prompt="consent")

    if not creds.refresh_token:
        raise SystemExit("Kein Refresh-Token erhalten – App-Zugriff unter "
                         "myaccount.google.com/permissions entfernen und erneut ausführen.")

    Secret(value=creds.to_json()).save(TOKEN_BLOCK, overwrite=True)
    print(f"Token gespeichert als Prefect Secret Block '{TOKEN_BLOCK}'.")


if __name__ == "__main__":
    main()
