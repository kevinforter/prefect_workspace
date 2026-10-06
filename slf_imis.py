"""
SLF IMIS -> Google Drive -> Databricks Bronze (-> Silver)

Eigenständige Datei – Prefect lädt das Skript ohne den Repo-Ordner im Python-Pfad,
deshalb sind die Drive-Helfer hier enthalten statt aus gdrive_upload.py importiert.

Flows:
  * slf_imis_live     – alle 6 h: SLF-API (letzte 24 h, alle Stationen) -> Drive (NDJSON) -> Bronze
  * slf_imis_history  – wöchentlich: Archiv-CSVs measurement-data.slf.ch -> Drive (nur geänderte) -> Bronze

Bronze lädt der Databricks Job `gdrive_to_bronze` (BRONZE_JOB_ID setzen),
danach optional Job `slf_imis_silver` (SILVER_JOB_ID setzen).

Datenquelle: WSL-Institut für Schnee- und Lawinenforschung SLF, IMIS-Messnetz
(doi:10.16904/envidat.406) – Nutzungsbedingungen beachten.
"""

from __future__ import annotations

import asyncio
import io
import json
import re
import threading
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import httpx
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_databricks import DatabricksCredentials
from prefect_databricks.flows import jobs_runs_submit_by_id_and_wait_for_completion

# --------------------------------------------------------------------------- #
# Konfiguration
# --------------------------------------------------------------------------- #
GDRIVE_ROOT_FOLDER_ID = "1anLG5HmPHSO1jknvM-iNTbQMNeQvXp1B"
GDRIVE_TOKEN_BLOCK = "gdrive-token"       # Prefect Secret-Block (JSON mit refresh_token)
GDRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
DATABRICKS_BLOCK = "databricks"
BRONZE_JOB_ID: int | None = None          # Job-ID von `gdrive_to_bronze`
SILVER_JOB_ID: int | None = None          # Job-ID von `slf_imis_silver`, None = nicht starten

DRIVE_PREFIX = "slf-imis"                 # Unterordner im Drive-Root-Ordner
SCHEMA = "workspace.slf"
LIVE_RETENTION_DAYS = 30                  # Live-Rohdateien in Drive nach Bronze-Load löschen; None = behalten

API_BASE = "https://measurement-api.slf.ch/public/api/imis"
ARCHIVE_BASE = "https://measurement-data.slf.ch/imis/"
USER_AGENT = "kevinforter-private-warehouse/1.0 (Prefect; IMIS ingestion)"
TIMEOUT = httpx.Timeout(60.0, read=300.0)
MAX_PARALLEL_DOWNLOADS = 4

# API-Endpunkt -> (Pfad, Query-Parameter)
LIVE_ENDPOINTS = {
    "stations": ("/stations", {}),
    "measurements": ("/measurements", {}),                      # 30-min, letzte 24 h
    "precipitation": ("/measurements-precipitation", {}),       # 10-min, letzte 24 h
    "daily_snow": ("/daily-snow", {"period_in_days": 3}),       # Tageswerte HS / HN_1D
}

# Archiv-Verzeichnis -> Ordner in Drive (unter DRIVE_PREFIX)
ARCHIVE_DIRS = {
    "data/by_station/": "history/by_station",
    "data/daily_snow_values/": "history/daily_snow",
}

# Bronze-Tabellen: eine pro Dateistruktur (Parameter für ingest_gdrive_to_bronze)
# source_path wird rekursiv gelesen (inkl. Unterordner wie date=YYYY-MM-DD)
BRONZE_LIVE = [
    dict(source_path=f"{DRIVE_PREFIX}/live/{name}", file_pattern="*.json", file_format="json",
         target_table=f"{SCHEMA}.bronze_imis_live_{name}")
    for name in LIVE_ENDPOINTS
]
BRONZE_HISTORY = [
    dict(source_path=f"{DRIVE_PREFIX}/history/by_station", file_pattern="???[0-9].csv",
         file_format="csv", target_table=f"{SCHEMA}.bronze_imis_history_measurements"),
    dict(source_path=f"{DRIVE_PREFIX}/history/by_station", file_pattern="*_pluvio.csv",
         file_format="csv", target_table=f"{SCHEMA}.bronze_imis_history_precipitation"),
    dict(source_path=f"{DRIVE_PREFIX}/history/daily_snow", file_pattern="*.csv",
         file_format="csv", target_table=f"{SCHEMA}.bronze_imis_history_daily_snow"),
]


def _http() -> httpx.AsyncClient:
    return httpx.AsyncClient(headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, follow_redirects=True)


# --------------------------------------------------------------------------- #
# Google Drive
# --------------------------------------------------------------------------- #
# Drive kennt keine Pfade, nur Ordner mit IDs. Die Helfer bilden "a/b/c.csv"
# auf Ordner unterhalb von GDRIVE_ROOT_FOLDER_ID ab und legen fehlende an.
# Die Google-Clients sind nicht threadsicher -> pro Aufruf ein eigener Service,
# Ordneranlage hinter einem Lock (sonst entstehen bei Parallelität Duplikate).
FOLDER_MIME = "application/vnd.google-apps.folder"
_CREDS: Credentials | None = None
_FOLDER_CACHE: dict[str, str] = {}
_FOLDER_LOCK = threading.Lock()


async def _gdrive_creds() -> Credentials:
    global _CREDS
    if _CREDS is None:
        value = (await Secret.load(GDRIVE_TOKEN_BLOCK)).get()
        info = value if isinstance(value, dict) else json.loads(value)
        _CREDS = Credentials.from_authorized_user_info(info, GDRIVE_SCOPES)
    return _CREDS


def _service(creds: Credentials):
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _q(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _find(svc, parent_id: str, name: str, folder: bool = False) -> dict | None:
    op = "=" if folder else "!="
    q = (f"name = '{_q(name)}' and '{parent_id}' in parents and trashed = false "
         f"and mimeType {op} '{FOLDER_MIME}'")
    files = svc.files().list(q=q, fields="files(id,name,size,appProperties)", pageSize=1).execute()
    return (files.get("files") or [None])[0]


def _ensure_folder(svc, path: str) -> str:
    path = path.strip("/")
    if not path:
        return GDRIVE_ROOT_FOLDER_ID
    with _FOLDER_LOCK:
        parent = GDRIVE_ROOT_FOLDER_ID
        walked: list[str] = []
        for part in path.split("/"):
            walked.append(part)
            key = "/".join(walked)
            if key not in _FOLDER_CACHE:
                found = _find(svc, parent, part, folder=True)
                if found is None:
                    found = svc.files().create(
                        body={"name": part, "mimeType": FOLDER_MIME, "parents": [parent]}, fields="id"
                    ).execute()
                _FOLDER_CACHE[key] = found["id"]
            parent = _FOLDER_CACHE[key]
        return parent


def _split(path: str) -> tuple[str, str]:
    folder, _, name = path.strip("/").rpartition("/")
    return folder, name


def _put_sync(creds, data: bytes, path: str, mime: str, props: dict[str, str] | None) -> str:
    svc = _service(creds)
    folder, name = _split(path)
    parent = _ensure_folder(svc, folder)
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=True)
    extra = {"appProperties": props} if props else {}
    existing = _find(svc, parent, name)
    if existing:  # gleicher Name im Ordner -> Inhalt ersetzen statt Duplikat
        return svc.files().update(fileId=existing["id"], body=extra, media_body=media, fields="id").execute()["id"]
    return svc.files().create(
        body={"name": name, "parents": [parent], **extra}, media_body=media, fields="id"
    ).execute()["id"]


def _head_sync(creds, path: str) -> dict | None:
    svc = _service(creds)
    folder, name = _split(path)
    return _find(svc, _ensure_folder(svc, folder), name)


async def upload_to_gdrive(
    data: bytes, path: str, mime: str = "application/octet-stream", props: dict[str, str] | None = None
) -> str:
    creds = await _gdrive_creds()
    await asyncio.to_thread(_put_sync, creds, data, path, mime, props)
    get_run_logger().info("Hochgeladen: drive:/%s (%.1f MB)", path, len(data) / 1e6)
    return path


async def _gdrive_head(path: str) -> dict | None:
    return await asyncio.to_thread(_head_sync, await _gdrive_creds(), path)


def _cleanup_live_sync(creds, older_than: datetime) -> int:
    """Löscht date=YYYY-MM-DD-Ordner unter live/<endpoint>/, die älter als older_than sind."""
    svc = _service(creds)
    deleted = 0
    for name in LIVE_ENDPOINTS:
        parent = _ensure_folder(svc, f"{DRIVE_PREFIX}/live/{name}")
        token = None
        while True:
            res = svc.files().list(
                q=f"'{parent}' in parents and trashed = false and mimeType = '{FOLDER_MIME}'",
                fields="nextPageToken, files(id,name)", pageSize=1000, pageToken=token,
            ).execute()
            for f in res.get("files", []):
                m = re.fullmatch(r"date=(\d{4}-\d{2}-\d{2})", f["name"])
                if m and datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc) < older_than:
                    svc.files().delete(fileId=f["id"]).execute()  # löscht Ordner inkl. Inhalt
                    deleted += 1
            token = res.get("nextPageToken")
            if not token:
                break
    return deleted


# --------------------------------------------------------------------------- #
# Databricks
# --------------------------------------------------------------------------- #
@flow(name="gdrive-to-bronze", log_prints=True)
async def ingest_gdrive_to_bronze(
    source_path: str,
    target_table: str,
    file_format: str = "csv",
    file_pattern: str = "*",
    csv_delimiter: str = ",",
    sheet_name: str = "0",
    full_refresh: bool = False,
    max_wait_seconds: int = 3600,
):
    """Startet den Databricks Job `gdrive_to_bronze` und wartet, bis er fertig ist."""
    if not BRONZE_JOB_ID:
        raise ValueError("BRONZE_JOB_ID ist nicht gesetzt (Job-ID von gdrive_to_bronze eintragen).")
    params = {
        "root_folder_id": GDRIVE_ROOT_FOLDER_ID,
        "source_path": source_path,
        "target_table": target_table,
        "file_format": file_format,
        "file_pattern": file_pattern,
        "csv_delimiter": csv_delimiter,
        "sheet_name": sheet_name,
        "full_refresh": str(full_refresh).lower(),
    }
    print(f"Starte Databricks Job {BRONZE_JOB_ID} mit {params}")
    return await jobs_runs_submit_by_id_and_wait_for_completion(
        databricks_credentials=await DatabricksCredentials.load(DATABRICKS_BLOCK),
        job_id=BRONZE_JOB_ID,
        notebook_params=params,
        max_wait_seconds=max_wait_seconds,
        poll_frequency_seconds=30,
    )


async def _run_bronze(specs: list[dict], csv_delimiter: str, full_refresh: bool) -> None:
    # nacheinander: der Job erlaubt standardmässig nur einen Lauf gleichzeitig
    for spec in specs:
        await ingest_gdrive_to_bronze(**spec, csv_delimiter=csv_delimiter, full_refresh=full_refresh)


async def _run_silver() -> None:
    if not SILVER_JOB_ID:
        return
    await jobs_runs_submit_by_id_and_wait_for_completion(
        databricks_credentials=await DatabricksCredentials.load(DATABRICKS_BLOCK),
        job_id=SILVER_JOB_ID,
        max_wait_seconds=3600,
        poll_frequency_seconds=30,
    )


# --------------------------------------------------------------------------- #
# Live (API)
# --------------------------------------------------------------------------- #
@task(retries=3, retry_delay_seconds=60)
async def fetch_live(name: str) -> bytes:
    path, params = LIVE_ENDPOINTS[name]
    async with _http() as http:
        r = await http.get(API_BASE + path, params=params)
        r.raise_for_status()
        records = r.json()
    get_run_logger().info("%s: %d Datensätze", name, len(records))
    # NDJSON: eine Zeile pro Datensatz
    return "\n".join(json.dumps(rec, ensure_ascii=False) for rec in records).encode()


@task(retries=2, retry_delay_seconds=30)
async def store_live(data: bytes, path: str) -> str:
    return await upload_to_gdrive(data, path, mime="application/json")


@task
async def cleanup_live(retention_days: int) -> int:
    older_than = datetime.now(timezone.utc) - timedelta(days=retention_days)
    deleted = await asyncio.to_thread(_cleanup_live_sync, await _gdrive_creds(), older_than)
    get_run_logger().info("Live-Ordner älter als %d Tage gelöscht: %d", retention_days, deleted)
    return deleted


@flow(name="slf-imis-live", log_prints=True)
async def slf_imis_live(
    load_bronze: bool = True,
    full_refresh: bool = False,
    retention_days: int | None = LIVE_RETENTION_DAYS,
):
    ts = datetime.now(timezone.utc)
    for name in LIVE_ENDPOINTS:
        data = await fetch_live(name)
        await store_live(
            data,
            f"{DRIVE_PREFIX}/live/{name}/date={ts:%Y-%m-%d}/{name}_{ts:%Y%m%dT%H%M%SZ}.json",
        )
    if load_bronze:
        await _run_bronze(BRONZE_LIVE, csv_delimiter=",", full_refresh=full_refresh)
        await _run_silver()
        # erst nach erfolgreichem Bronze-Load aufräumen: Bronze ist das Archiv der Live-Daten
        if retention_days:
            await cleanup_live(retention_days)


# --------------------------------------------------------------------------- #
# History (Archiv-CSVs)
# --------------------------------------------------------------------------- #
@task(retries=2, retry_delay_seconds=30)
async def list_archive_files(source_dir: str) -> list[str]:
    """CSV-Dateien direkt in einem Archiv-Verzeichnis (h5ai-Listing, nicht rekursiv)."""
    url = urljoin(ARCHIVE_BASE, source_dir)
    async with _http() as http:
        html = (await http.get(url)).raise_for_status().text
    urls = {urljoin(url, h) for h in re.findall(r'href="([^"]+\.csv)"', html, flags=re.I)}
    return sorted(u for u in urls if u.rsplit("/", 1)[0] + "/" == url)


@task(retries=3, retry_delay_seconds=60)
async def sync_archive_file(url: str, drive_dir: str, force: bool = False) -> str:
    """Kopiert eine Archivdatei nach Drive – nur wenn sie dort fehlt oder sich Grösse
    bzw. Änderungsdatum an der Quelle geändert haben. Quell-Metadaten werden als
    appProperties an der Drive-Datei gespeichert."""
    path = f"{DRIVE_PREFIX}/{drive_dir}/{url.rsplit('/', 1)[1]}"
    async with _http() as http:
        src = (await http.head(url)).raise_for_status()
        src_size = int(src.headers.get("content-length", -1))
        src_mod = parsedate_to_datetime(src.headers["last-modified"])

        dst = await _gdrive_head(path)
        props = (dst or {}).get("appProperties") or {}
        if (
            not force
            and dst
            and props.get("source_size") == str(src_size)
            and props.get("source_last_modified")
            and datetime.fromisoformat(props["source_last_modified"]) >= src_mod
        ):
            return "skipped"

        data = (await http.get(url)).raise_for_status().content
    await upload_to_gdrive(
        data,
        path,
        mime="text/csv",
        props={"source_size": str(src_size), "source_last_modified": src_mod.isoformat()},
    )
    return "uploaded"


@flow(name="slf-imis-history", log_prints=True)
async def slf_imis_history(
    force: bool = False,
    load_bronze: bool = True,
    full_refresh: bool = False,
    csv_delimiter: str = ",",
):
    sem = asyncio.Semaphore(MAX_PARALLEL_DOWNLOADS)

    async def limited(url: str, drive_dir: str) -> str:
        async with sem:
            return await sync_archive_file(url, drive_dir, force)

    results: list[str] = []
    for source_dir, drive_dir in ARCHIVE_DIRS.items():
        urls = await list_archive_files(source_dir)
        print(f"{source_dir}: {len(urls)} Dateien")
        results += await asyncio.gather(*(limited(u, drive_dir) for u in urls))

    summary = {s: results.count(s) for s in ("uploaded", "skipped")}
    print(f"Archiv -> Drive: {summary}")

    if load_bronze and (summary["uploaded"] or full_refresh):
        await _run_bronze(BRONZE_HISTORY, csv_delimiter=csv_delimiter, full_refresh=full_refresh)
        await _run_silver()
    return summary


if __name__ == "__main__":
    asyncio.run(slf_imis_live(load_bronze=False))