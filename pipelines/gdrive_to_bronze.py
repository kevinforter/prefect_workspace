# Databricks notebook source
# MAGIC %md
# MAGIC # Google Drive → Bronze
# MAGIC
# MAGIC Liest Dateien aus einem Google-Drive-Ordner (rekursiv) und hängt sie an eine Bronze-Tabelle an.
# MAGIC Gleiche Konventionen wie `r2_to_bronze`:
# MAGIC - inkrementell über `<catalog>.<schema>._gdrive_ingestion_log`
# MAGIC - CSV/Excel/JSON komplett als String, Spaltennamen Delta-kompatibel
# MAGIC - Metadaten `_source_file`, `_source_modified_at`, `_ingested_at`
# MAGIC - einzelne Fehler → übrige Dateien werden geladen, Job endet aber als *Failed*
# MAGIC
# MAGIC Voraussetzung: Secret Scope `gdrive`, Key `token` (Inhalt wie Prefect-Secret `gdrive-token`).

# COMMAND ----------

# MAGIC %pip install google-api-python-client google-auth openpyxl --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("root_folder_id", "1anLG5HmPHSO1jknvM-iNTbQMNeQvXp1B")
dbutils.widgets.text("source_path", "slf-imis/history/daily_snow")
dbutils.widgets.text("file_pattern", "*")
dbutils.widgets.dropdown("file_format", "csv", ["csv", "json", "xlsx", "parquet"])
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("csv_delimiter", ",")
dbutils.widgets.text("sheet_name", "0")
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"])

ROOT = dbutils.widgets.get("root_folder_id")
SOURCE_PATH = dbutils.widgets.get("source_path").strip("/")
PATTERN = dbutils.widgets.get("file_pattern")
FMT = dbutils.widgets.get("file_format")
TARGET = dbutils.widgets.get("target_table")
DELIM = dbutils.widgets.get("csv_delimiter")
SHEET = dbutils.widgets.get("sheet_name")
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"

assert TARGET.count(".") == 2, "target_table muss <catalog>.<schema>.<tabelle> sein"
CATALOG, SCHEMA, _ = TARGET.split(".")
LOG_TABLE = f"{CATALOG}.{SCHEMA}._gdrive_ingestion_log"

# COMMAND ----------

import fnmatch
import io
import json
import re
from datetime import datetime, timezone

import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

info = json.loads(dbutils.secrets.get("gdrive", "token"))
creds = Credentials.from_authorized_user_info(info, ["https://www.googleapis.com/auth/drive.file"])
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

FOLDER_MIME = "application/vnd.google-apps.folder"


def _q(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def list_children(folder_id: str) -> list[dict]:
    items, token = [], None
    while True:
        res = drive.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id,name,mimeType,size,modifiedTime)",
            pageSize=1000,
            pageToken=token,
        ).execute()
        items += res.get("files", [])
        token = res.get("nextPageToken")
        if not token:
            return items


def resolve_folder(path: str) -> str:
    folder_id = ROOT
    for part in [p for p in path.split("/") if p]:
        res = drive.files().list(
            q=f"name = '{_q(part)}' and '{folder_id}' in parents and trashed = false and mimeType = '{FOLDER_MIME}'",
            fields="files(id)",
            pageSize=1,
        ).execute()
        if not res.get("files"):
            raise FileNotFoundError(f"Drive-Ordner nicht gefunden: {path} (bei '{part}')")
        folder_id = res["files"][0]["id"]
    return folder_id


def walk(folder_id: str, prefix: str):
    for f in list_children(folder_id):
        rel = f"{prefix}/{f['name']}" if prefix else f["name"]
        if f["mimeType"] == FOLDER_MIME:
            yield from walk(f["id"], rel)
        else:
            yield {**f, "path": rel}


def download(file_id: str) -> bytes:
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id))
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()

# COMMAND ----------

# Ingestion-Log und Full Refresh
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
        file_id STRING, file_path STRING, modified_time STRING,
        target_table STRING, row_count BIGINT, ingested_at TIMESTAMP
    )
""")

if FULL_REFRESH:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
    spark.sql(f"DELETE FROM {LOG_TABLE} WHERE target_table = '{TARGET}'")
    print(f"Full Refresh: {TARGET} und Log-Einträge gelöscht")

logged = {
    r.file_id: r.modified_time
    for r in spark.sql(f"""
        SELECT file_id, max(modified_time) AS modified_time
        FROM {LOG_TABLE} WHERE target_table = '{TARGET}' GROUP BY file_id
    """).collect()
}

all_files = list(walk(resolve_folder(SOURCE_PATH), SOURCE_PATH))
matching = [f for f in all_files if fnmatch.fnmatch(f["name"], PATTERN)]
todo = [f for f in matching if f["id"] not in logged or f["modifiedTime"] > logged[f["id"]]]
print(f"{len(matching)} Dateien passen auf '{PATTERN}', davon neu oder geändert: {len(todo)}")

# COMMAND ----------

def clean_column(name: str) -> str:
    col = re.sub(r"[^0-9a-z_]", "_", str(name).strip().lower())
    return col or "_unnamed"


def to_pandas(data: bytes) -> pd.DataFrame:
    if FMT == "csv":
        return pd.read_csv(io.BytesIO(data), sep=DELIM, dtype=str, keep_default_na=False)
    if FMT == "xlsx":
        sheet = int(SHEET) if SHEET.isdigit() else SHEET
        return pd.read_excel(io.BytesIO(data), sheet_name=sheet, dtype=str)
    if FMT == "parquet":
        return pd.read_parquet(io.BytesIO(data))
    if FMT == "json":
        text = data.decode("utf-8").strip()
        if text.startswith("["):
            records = json.loads(text)
        else:  # NDJSON
            records = [json.loads(line) for line in text.splitlines() if line.strip()]
        rows = [
            {
                k: (json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list))
                    else None if v is None else str(v))
                for k, v in rec.items()
            }
            for rec in records
        ]
        return pd.DataFrame(rows)
    raise ValueError(f"Unbekanntes Format: {FMT}")


def to_spark(pdf: pd.DataFrame):
    pdf.columns = [clean_column(c) for c in pdf.columns]
    if FMT == "parquet":
        return spark.createDataFrame(pdf)
    # Bronze = Rohdaten: alles als String, fehlende Werte als NULL
    pdf = pdf.astype(object).where(pdf.notna(), None)
    schema = StructType([StructField(c, StringType()) for c in pdf.columns])
    return spark.createDataFrame(pdf, schema=schema)

# COMMAND ----------

errors, log_rows = [], []

for f in todo:
    try:
        pdf = to_pandas(download(f["id"]))
        rows = len(pdf)
        if rows:
            sdf = (
                to_spark(pdf)
                .withColumn("_source_file", F.lit(f["path"]))
                .withColumn("_source_modified_at", F.to_timestamp(F.lit(f["modifiedTime"])))
                .withColumn("_ingested_at", F.current_timestamp())
            )
            sdf.write.mode("append").option("mergeSchema", "true").saveAsTable(TARGET)
        log_rows.append((f["id"], f["path"], f["modifiedTime"], TARGET, rows, datetime.now(timezone.utc)))
        print(f"OK   {f['path']} ({rows} Zeilen)")
    except Exception as e:  # einzelne Datei überspringen, Rest weiterladen
        errors.append((f["path"], repr(e)))
        print(f"FEHLER {f['path']}: {e!r}")

if log_rows:
    spark.createDataFrame(
        log_rows,
        "file_id STRING, file_path STRING, modified_time STRING, target_table STRING, row_count BIGINT, ingested_at TIMESTAMP",
    ).write.mode("append").saveAsTable(LOG_TABLE)

print(f"Geladen: {len(log_rows)}, Fehler: {len(errors)}")
if errors:
    raise RuntimeError(f"{len(errors)} Datei(en) fehlgeschlagen: {errors[:5]}")
