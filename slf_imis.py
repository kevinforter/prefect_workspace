"""
SLF IMIS -> R2 -> Databricks Bronze (-> Silver)

Eigenständige Datei – enthält eine Kopie der Helfer aus `r2_ingestion.py`:
  * upload_to_r2(data, key)      – Rohdatei nach R2
  * ingest_r2_to_bronze(...)     – startet Databricks Job `r2_to_bronze` und wartet

Flows:
  * slf_imis_live     – alle 6 h: SLF-API (letzte 24 h, alle Stationen) -> R2 (NDJSON) -> Bronze
  * slf_imis_history  – wöchentlich: Archiv-CSVs measurement-data.slf.ch -> R2 (nur geänderte) -> Bronze

Danach optional Databricks Job `slf_imis_silver` (SILVER_JOB_ID setzen).

Datenquelle: WSL-Institut für Schnee- und Lawinenforschung SLF, IMIS-Messnetz
(doi:10.16904/envidat.406) – Nutzungsbedingungen beachten.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import boto3
import httpx
from botocore.exceptions import ClientError
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_databricks import DatabricksCredentials
from prefect_databricks.flows import jobs_runs_submit_by_id_and_wait_for_completion

# --------------------------------------------------------------------------- #
# Konfiguration
# --------------------------------------------------------------------------- #
# Eigenständig (kein Import aus r2_ingestion.py): Prefect lädt das Skript ohne
# den Repo-Ordner im Python-Pfad, Imports von Nachbardateien schlagen fehl.
R2_ENDPOINT = "https://196684d5990e4e8c3f5ce5ba0dc956c3.r2.cloudflarestorage.com"
R2_BUCKET = "blob"
R2_ACCESS_KEY_BLOCK = "r2-access-key"     # Prefect Secret-Blocks (existieren bereits)
R2_SECRET_KEY_BLOCK = "r2-secret-key"
DATABRICKS_BLOCK = "databricks"
BRONZE_JOB_ID: int | None = None          # gleiche Job-ID wie in r2_ingestion.py (Job `r2_to_bronze`)
SILVER_JOB_ID: int | None = None          # Job-ID von `slf_imis_silver`, None = nicht starten

R2_PREFIX = "slf-imis"
SCHEMA = "workspace.slf"

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

# Archiv-Verzeichnis -> Ordner in R2
ARCHIVE_DIRS = {
    "data/by_station/": "history/by_station",
    "data/daily_snow_values/": "history/daily_snow",
}

# Bronze-Tabellen: eine pro Dateistruktur (Parameter für ingest_r2_to_bronze)
BRONZE_LIVE = [
    dict(source_prefix=f"{R2_PREFIX}/live/{name}/", file_pattern="*.json", file_format="json",
         target_table=f"{SCHEMA}.bronze_imis_live_{name}")
    for name in LIVE_ENDPOINTS
]
BRONZE_HISTORY = [
    dict(source_prefix=f"{R2_PREFIX}/history/by_station/", file_pattern="???[0-9].csv",
         file_format="csv", target_table=f"{SCHEMA}.bronze_imis_history_measurements"),
    dict(source_prefix=f"{R2_PREFIX}/history/by_station/", file_pattern="*_pluvio.csv",
         file_format="csv", target_table=f"{SCHEMA}.bronze_imis_history_precipitation"),
    dict(source_prefix=f"{R2_PREFIX}/history/daily_snow/", file_pattern="*.csv",
         file_format="csv", target_table=f"{SCHEMA}.bronze_imis_history_daily_snow"),
]


def _http() -> httpx.AsyncClient:
    return httpx.AsyncClient(headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, follow_redirects=True)


# --------------------------------------------------------------------------- #
# R2 + Databricks (wie in r2_ingestion.py)
# --------------------------------------------------------------------------- #
_R2 = None


async def _r2_client():
    global _R2
    if _R2 is None:
        access_key = (await Secret.load(R2_ACCESS_KEY_BLOCK)).get()
        secret_key = (await Secret.load(R2_SECRET_KEY_BLOCK)).get()
        _R2 = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )
    return _R2


async def upload_to_r2(data: bytes, key: str) -> str:
    client = await _r2_client()
    await asyncio.to_thread(client.put_object, Bucket=R2_BUCKET, Key=key, Body=data)
    get_run_logger().info("Hochgeladen: s3a://%s/%s (%.1f MB)", R2_BUCKET, key, len(data) / 1e6)
    return f"s3a://{R2_BUCKET}/{key}"


@flow(name="r2-to-bronze", log_prints=True)
async def ingest_r2_to_bronze(
    source_prefix: str,
    target_table: str,
    file_format: str = "csv",
    file_pattern: str = "*",
    csv_delimiter: str = ",",
    sheet_name: str = "0",
    full_refresh: bool = False,
    max_wait_seconds: int = 3600,
):
    """Startet den Databricks Job `r2_to_bronze` und wartet, bis er fertig ist."""
    if not BRONZE_JOB_ID:
        raise ValueError("BRONZE_JOB_ID ist nicht gesetzt (Job-ID von r2_to_bronze eintragen).")
    params = {
        "source_prefix": source_prefix,
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
    # nacheinander: der Job `r2_to_bronze` erlaubt standardmässig nur einen Lauf gleichzeitig
    for spec in specs:
        await ingest_r2_to_bronze(**spec, csv_delimiter=csv_delimiter, full_refresh=full_refresh)


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
    # NDJSON: eine Zeile pro Datensatz (liest Spark ohne multiLine)
    return "\n".join(json.dumps(rec, ensure_ascii=False) for rec in records).encode()


@flow(name="slf-imis-live", log_prints=True)
async def slf_imis_live(load_bronze: bool = True, full_refresh: bool = False):
    ts = datetime.now(timezone.utc)
    for name in LIVE_ENDPOINTS:
        data = await fetch_live(name)
        await upload_to_r2(
            data,
            key=f"{R2_PREFIX}/live/{name}/date={ts:%Y-%m-%d}/{name}_{ts:%Y%m%dT%H%M%SZ}.json",
        )
    if load_bronze:
        await _run_bronze(BRONZE_LIVE, csv_delimiter=",", full_refresh=full_refresh)
        await _run_silver()


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


async def _r2_head(key: str) -> dict | None:
    client = await _r2_client()
    try:
        return await asyncio.to_thread(client.head_object, Bucket=R2_BUCKET, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


@task(retries=3, retry_delay_seconds=60)
async def sync_archive_file(url: str, r2_dir: str, force: bool = False) -> str:
    """Kopiert eine Archivdatei nach R2 – nur wenn sie dort fehlt, eine andere
    Grösse hat oder an der Quelle neuer ist als die Kopie in R2."""
    key = f"{R2_PREFIX}/{r2_dir}/{url.rsplit('/', 1)[1]}"
    async with _http() as http:
        src = (await http.head(url)).raise_for_status()
        src_size = int(src.headers.get("content-length", -1))
        src_mod = parsedate_to_datetime(src.headers["last-modified"])

        dst = await _r2_head(key)
        if not force and dst and dst["ContentLength"] == src_size and dst["LastModified"] >= src_mod:
            return "skipped"

        data = (await http.get(url)).raise_for_status().content
    await upload_to_r2(data, key=key)
    return "uploaded"


@flow(name="slf-imis-history", log_prints=True)
async def slf_imis_history(
    force: bool = False,
    load_bronze: bool = True,
    full_refresh: bool = False,
    csv_delimiter: str = ",",
):
    sem = asyncio.Semaphore(MAX_PARALLEL_DOWNLOADS)

    async def limited(url: str, r2_dir: str) -> str:
        async with sem:
            return await sync_archive_file(url, r2_dir, force)

    results: list[str] = []
    for source_dir, r2_dir in ARCHIVE_DIRS.items():
        urls = await list_archive_files(source_dir)
        print(f"{source_dir}: {len(urls)} Dateien")
        results += await asyncio.gather(*(limited(u, r2_dir) for u in urls))

    summary = {s: results.count(s) for s in ("uploaded", "skipped")}
    print(f"Archiv -> R2: {summary}")

    if load_bronze and (summary["uploaded"] or full_refresh):
        await _run_bronze(BRONZE_HISTORY, csv_delimiter=csv_delimiter, full_refresh=full_refresh)
        await _run_silver()
    return summary


if __name__ == "__main__":
    asyncio.run(slf_imis_live(load_bronze=False))