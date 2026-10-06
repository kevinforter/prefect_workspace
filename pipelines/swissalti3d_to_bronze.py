# Databricks notebook source
# MAGIC %md
# MAGIC # swissALTI3D: Google Drive → Tabellen
# MAGIC
# MAGIC Liest die GeoTIFF-Tiles (2 m) aus Drive `data_lake/raw/swissalti3d/` und schreibt zwei Delta-Tabellen:
# MAGIC
# MAGIC | Tabelle | Inhalt | Zeilen |
# MAGIC |---|---|---|
# MAGIC | `swissalti3d_tiles` | Katalog: ein Eintrag pro Tile (Jahr, Lage, Höhenstatistik) | ~1 pro km² |
# MAGIC | `swissalti3d_terrain_<n>m` | Geländemerkmale pro Rasterzelle (Höhe, Neigung, Exposition) | 400 pro km² bei 50 m |
# MAGIC
# MAGIC Neigung und Exposition werden auf dem **2-m-Raster** berechnet und danach pro Zelle zusammengefasst
# MAGIC (Mittel, Max, P90, Anteil 30–45°). So bleibt die Steilheit erhalten, obwohl die Tabelle gröber ist.
# MAGIC
# MAGIC **Inkrementell:** Verarbeitete Dateien stehen in `_gdrive_ingestion_log`. Geänderte Dateien (neuere `modifiedTime`) werden ersetzt.
# MAGIC `full_refresh=true` löscht beide Tabellen und die Log-Einträge.
# MAGIC
# MAGIC **Koordinaten:** LV95 (EPSG:2056). Für den Join mit Unfallpunkten:
# MAGIC `cell_e = floor(e / cell_size_m) * cell_size_m`, analog `cell_n`.

# COMMAND ----------

# MAGIC %pip install --quiet rasterio google-api-python-client google-auth
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "swisstopo")
dbutils.widgets.text("root_folder_id", "1anLG5HmPHSO1jknvM-iNTbQMNeQvXp1B")
dbutils.widgets.text("source_folder_id", "")          # von Prefect übergeben
dbutils.widgets.text("source_folder_name", "swissalti3d")  # Fallback, falls keine ID
dbutils.widgets.text("file_pattern", "swissalti3d_*.tif")
dbutils.widgets.text("cell_size_m", "50")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])
dbutils.widgets.text("max_files", "0")                 # 0 = alle neuen Dateien
dbutils.widgets.text("batch_size", "200")
dbutils.widgets.text("max_workers", "8")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ROOT_FOLDER_ID = dbutils.widgets.get("root_folder_id").strip()
FOLDER_ID = dbutils.widgets.get("source_folder_id").strip()
FOLDER_NAME = dbutils.widgets.get("source_folder_name").strip()
FILE_PATTERN = dbutils.widgets.get("file_pattern")
CELL = int(dbutils.widgets.get("cell_size_m"))
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
MAX_FILES = int(dbutils.widgets.get("max_files") or 0)
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
MAX_WORKERS = int(dbutils.widgets.get("max_workers"))

FQ = f"{CATALOG}.{SCHEMA}"
T_TILES = f"{FQ}.swissalti3d_tiles"
T_TERRAIN = f"{FQ}.swissalti3d_terrain_{CELL}m"
T_LOG = f"{FQ}._gdrive_ingestion_log"

print(T_TILES, T_TERRAIN, T_LOG, sep="\n")

# COMMAND ----------

# MAGIC %md ## Tabellen anlegen

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")

if FULL_REFRESH:
    spark.sql(f"DROP TABLE IF EXISTS {T_TILES}")
    spark.sql(f"DROP TABLE IF EXISTS {T_TERRAIN}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_LOG} (
  file_id        STRING,
  file_name      STRING,
  modified_time  TIMESTAMP,
  target_table   STRING,
  ingested_at    TIMESTAMP
)
""")

if FULL_REFRESH:
    spark.sql(f"DELETE FROM {T_LOG} WHERE target_table IN ('{T_TILES}', '{T_TERRAIN}')")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_TILES} (
  tile_id              STRING  COMMENT 'z.B. 2501-1120 (E-km, N-km der unteren linken Ecke)',
  year                 INT     COMMENT 'Aktualitätsjahr des Tiles',
  tile_e_km            INT,
  tile_n_km            INT,
  resolution_m         DOUBLE,
  width_px             INT,
  height_px            INT,
  crs                  STRING,
  e_min                DOUBLE,
  n_min                DOUBLE,
  e_max                DOUBLE,
  n_max                DOUBLE,
  elev_min             DOUBLE,
  elev_max             DOUBLE,
  elev_mean            DOUBLE,
  nodata_share         DOUBLE,
  _source_file         STRING,
  _source_file_id      STRING,
  _source_modified_at  TIMESTAMP,
  _ingested_at         TIMESTAMP
)
CLUSTER BY (tile_e_km, tile_n_km)
COMMENT 'swissALTI3D Tile-Katalog (Bronze)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_TERRAIN} (
  cell_id              STRING  COMMENT 'E<cell_e>_N<cell_n>',
  cell_e               INT     COMMENT 'LV95 E der unteren linken Zellecke (m)',
  cell_n               INT     COMMENT 'LV95 N der unteren linken Zellecke (m)',
  center_e             DOUBLE,
  center_n             DOUBLE,
  cell_size_m          INT,
  tile_id              STRING,
  year                 INT,
  elev_mean            DOUBLE,
  elev_min             DOUBLE,
  elev_max             DOUBLE,
  elev_std             DOUBLE  COMMENT 'Rauigkeit',
  slope_mean           DOUBLE  COMMENT 'Grad, aus 2-m-Raster',
  slope_p90            DOUBLE,
  slope_max            DOUBLE,
  share_slope_30_45    DOUBLE  COMMENT 'Anteil Pixel 30–45° (typisches Anrissgebiet)',
  share_slope_ge_30    DOUBLE,
  aspect_mean_deg      DOUBLE  COMMENT 'zirkuläres Mittel, 0=N, 90=E; NULL wenn flach',
  northness            DOUBLE  COMMENT 'Mittel cos(aspect), +1 = Nordhang',
  eastness             DOUBLE  COMMENT 'Mittel sin(aspect), +1 = Osthang',
  valid_share          DOUBLE,
  _source_file         STRING,
  _source_file_id      STRING,
  _source_modified_at  TIMESTAMP,
  _ingested_at         TIMESTAMP
)
CLUSTER BY (cell_e, cell_n)
COMMENT 'swissALTI3D Geländemerkmale pro Rasterzelle'
""")

# COMMAND ----------

# MAGIC %md ## Google Drive

# COMMAND ----------

import fnmatch
import io
import json
import threading
import time
from datetime import datetime, timezone

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

_TOKEN = json.loads(dbutils.secrets.get("gdrive", "token"))  # wie gdrive_to_bronze
_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
_local = threading.local()


def drive():
    if not hasattr(_local, "svc"):
        creds = Credentials.from_authorized_user_info(_TOKEN, _SCOPES)
        _local.svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _local.svc


def with_retries(fn, attempts=5, delay=2.0):
    for i in range(attempts):
        try:
            return fn()
        except HttpError as e:
            if e.resp.status not in (403, 429, 500, 502, 503, 504) or i == attempts - 1:
                raise
        except (OSError, TimeoutError):
            if i == attempts - 1:
                raise
        time.sleep(delay * 2**i)


def resolve_folder_id():
    if FOLDER_ID:
        return FOLDER_ID
    q = (f"name = '{FOLDER_NAME}' and '{ROOT_FOLDER_ID}' in parents "
         "and mimeType = 'application/vnd.google-apps.folder' and trashed = false")
    res = drive().files().list(q=q, fields="files(id, name)").execute()["files"]
    if len(res) != 1:
        raise ValueError(f"Ordner '{FOLDER_NAME}' {len(res)}x unter {ROOT_FOLDER_ID} gefunden – source_folder_id setzen.")
    return res[0]["id"]


def list_drive_files(folder_id):
    files, token = [], None
    while True:
        res = with_retries(lambda: drive().files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, modifiedTime, size)",
            pageSize=1000,
            pageToken=token,
        ).execute())
        files += [f for f in res.get("files", []) if fnmatch.fnmatch(f["name"], FILE_PATTERN)]
        token = res.get("nextPageToken")
        if not token:
            return files


def download_bytes(file_id):
    buf = io.BytesIO()
    req = drive().files().get_media(fileId=file_id)
    dl = MediaIoBaseDownload(buf, req, chunksize=16 << 20)
    done = False
    while not done:
        _, done = dl.next_chunk(num_retries=3)
    return buf.getvalue()


folder_id = resolve_folder_id()
drive_files = list_drive_files(folder_id)
print(f"{len(drive_files)} Dateien in Drive-Ordner {folder_id}")

# COMMAND ----------

# MAGIC %md ## Neue / geänderte Dateien bestimmen

# COMMAND ----------

def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


logged = {
    r.file_id: r.modified_time
    for r in spark.sql(f"""
        SELECT file_id, max(modified_time) AS modified_time
        FROM {T_LOG}
        WHERE target_table = '{T_TERRAIN}'
        GROUP BY file_id
    """).collect()
}

todo = []
for f in sorted(drive_files, key=lambda f: f["name"]):
    mod = parse_ts(f["modifiedTime"])
    prev = logged.get(f["id"])
    if prev is None or mod.replace(tzinfo=None) > prev.replace(tzinfo=None):
        todo.append({**f, "modified": mod})

if MAX_FILES:
    todo = todo[:MAX_FILES]

print(f"{len(logged)} bereits verarbeitet, {len(todo)} werden jetzt verarbeitet")

# COMMAND ----------

# MAGIC %md ## Rasterverarbeitung

# COMMAND ----------

import re
import warnings

import numpy as np
import pandas as pd
import rasterio
from rasterio.io import MemoryFile

TILE_RE = re.compile(r"swissalti3d_(\d{4})_(\d{4})-(\d{4})_([\d.]+)_2056_\d+\.tif$")


def terrain_derivatives(z, res):
    """Neigung (Grad) und Exposition (Grad, 0=N, 90=E) aus Höhenraster. NaN bleibt NaN."""
    dz_drow, dz_dcol = np.gradient(z, res)
    dz_de = dz_dcol
    dz_dn = -dz_drow  # Zeilen laufen nach Süden
    slope = np.degrees(np.arctan(np.hypot(dz_de, dz_dn)))
    aspect = np.degrees(np.arctan2(-dz_de, -dz_dn)) % 360.0  # Richtung hangabwärts
    return slope, aspect


def blocks(a, k):
    """(H, W) -> (H/k, W/k, k*k)"""
    h, w = a.shape
    return a.reshape(h // k, k, w // k, k).swapaxes(1, 2).reshape(h // k, w // k, k * k)


def process_tile(name, content, cell_size):
    m = TILE_RE.search(name)
    if not m:
        raise ValueError(f"Unerwarteter Dateiname: {name}")
    year, e_km, n_km = int(m[1]), int(m[2]), int(m[3])
    tile_id = f"{e_km}-{n_km}"

    with MemoryFile(content) as mem, mem.open() as src:
        z = src.read(1, masked=True).astype("float64").filled(np.nan)
        res = float(src.res[0])
        left, bottom, right, top = src.bounds
        crs = src.crs.to_string() if src.crs else None
        width, height = src.width, src.height

    valid = np.isfinite(z)
    tile_row = {
        "tile_id": tile_id, "year": year, "tile_e_km": e_km, "tile_n_km": n_km,
        "resolution_m": res, "width_px": width, "height_px": height, "crs": crs,
        "e_min": left, "n_min": bottom, "e_max": right, "n_max": top,
        "elev_min": float(np.nanmin(z)) if valid.any() else None,
        "elev_max": float(np.nanmax(z)) if valid.any() else None,
        "elev_mean": float(np.nanmean(z)) if valid.any() else None,
        "nodata_share": float(1 - valid.mean()),
    }

    k = cell_size / res
    if abs(k - round(k)) > 1e-9 or height % round(k) or width % round(k):
        raise ValueError(f"cell_size_m={cell_size} passt nicht zu {res} m / {width}x{height} px")
    k = int(round(k))

    slope, aspect = terrain_derivatives(z, res)
    # Flache Pixel haben keine sinnvolle Exposition
    sloped = np.where(slope >= 1.0, 1.0, np.nan)
    cos_a = np.cos(np.radians(aspect)) * sloped
    sin_a = np.sin(np.radians(aspect)) * sloped

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)  # leere Zellen
        zb, sb = blocks(z, k), blocks(slope, k)
        cb, snb = blocks(cos_a, k), blocks(sin_a, k)
        valid_share = np.isfinite(zb).mean(axis=2)
        s_valid = np.isfinite(sb).sum(axis=2)
        north, east = np.nanmean(cb, axis=2), np.nanmean(snb, axis=2)
        stats = {
            "elev_mean": np.nanmean(zb, axis=2),
            "elev_min": np.nanmin(zb, axis=2),
            "elev_max": np.nanmax(zb, axis=2),
            "elev_std": np.nanstd(zb, axis=2),
            "slope_mean": np.nanmean(sb, axis=2),
            "slope_p90": np.nanpercentile(sb, 90, axis=2),
            "slope_max": np.nanmax(sb, axis=2),
            "share_slope_30_45": np.where(
                s_valid > 0, ((sb >= 30) & (sb <= 45)).sum(axis=2) / np.maximum(s_valid, 1), np.nan),
            "share_slope_ge_30": np.where(
                s_valid > 0, (sb >= 30).sum(axis=2) / np.maximum(s_valid, 1), np.nan),
            "aspect_mean_deg": np.where(
                np.hypot(north, east) > 0.05, np.degrees(np.arctan2(east, north)) % 360.0, np.nan),
            "northness": north,
            "eastness": east,
            "valid_share": valid_share,
        }

    rows, cols = valid_share.shape
    ii, jj = np.meshgrid(np.arange(rows), np.arange(cols), indexing="ij")
    cell_e = (left + jj * cell_size).round().astype("int64")
    cell_n = (top - (ii + 1) * cell_size).round().astype("int64")

    df = pd.DataFrame({
        "cell_e": cell_e.ravel(), "cell_n": cell_n.ravel(),
        **{c: v.ravel() for c, v in stats.items()},
    })
    df = df[df["valid_share"] > 0].copy()
    df["cell_id"] = "E" + df["cell_e"].astype(str) + "_N" + df["cell_n"].astype(str)
    df["center_e"] = df["cell_e"] + cell_size / 2
    df["center_n"] = df["cell_n"] + cell_size / 2
    df["cell_size_m"] = cell_size
    df["tile_id"] = tile_id
    df["year"] = year
    return tile_row, df

# COMMAND ----------

# MAGIC %md ## Laden in Batches

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import functions as F

TILES_COLS = spark.table(T_TILES).columns
TERRAIN_COLS = spark.table(T_TERRAIN).columns


def handle(f):
    try:
        content = with_retries(lambda: download_bytes(f["id"]))
        tile_row, df = process_tile(f["name"], content, CELL)
        meta = {
            "_source_file": f["name"],
            "_source_file_id": f["id"],
            "_source_modified_at": f["modified"].replace(tzinfo=None),
        }
        tile_row.update(meta)
        for c, v in meta.items():
            df[c] = v
        return f, tile_row, df, None
    except Exception as e:  # einzelne Tiles dürfen scheitern
        return f, None, None, f"{type(e).__name__}: {e}"


def py_rows(pdf, cols):
    """pandas -> Liste von Tupeln mit Python-Typen, NaN -> None (wird zu NULL)."""
    out = []
    for rec in pdf[cols].astype(object).itertuples(index=False, name=None):
        out.append(tuple(
            None if v is None or (isinstance(v, float) and v != v)
            else int(v) if isinstance(v, np.integer)
            else float(v) if isinstance(v, np.floating)
            else v
            for v in rec
        ))
    return out


def to_spark(pdf, table, cols):
    pdf = pdf.copy()
    pdf["_ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    return spark.createDataFrame(py_rows(pdf, cols), schema=spark.table(table).schema)


failed = []
done = 0
for start in range(0, len(todo), BATCH_SIZE):
    batch = todo[start:start + BATCH_SIZE]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        results = list(pool.map(handle, batch))

    ok = [(f, t, d) for f, t, d, err in results if err is None]
    failed += [(f["name"], err) for f, _, _, err in results if err is not None]
    if not ok:
        continue

    ids = [f["id"] for f, _, _ in ok]
    spark.createDataFrame([(i,) for i in ids], "file_id STRING").createOrReplaceTempView("_batch_ids")
    # Idempotent: alte Zeilen derselben Dateien entfernen (Abbruch / geänderte Datei)
    for t in (T_TILES, T_TERRAIN):
        spark.sql(f"DELETE FROM {t} WHERE _source_file_id IN (SELECT file_id FROM _batch_ids)")

    to_spark(pd.DataFrame([t for _, t, _ in ok]), T_TILES, TILES_COLS) \
        .write.mode("append").saveAsTable(T_TILES)
    to_spark(pd.concat([d for _, _, d in ok], ignore_index=True), T_TERRAIN, TERRAIN_COLS) \
        .write.mode("append").saveAsTable(T_TERRAIN)

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    log_rows = [
        (f["id"], f["name"], f["modified"].replace(tzinfo=None), t, now)
        for f, _, _ in ok for t in (T_TILES, T_TERRAIN)
    ]
    spark.createDataFrame(log_rows, spark.table(T_LOG).schema).write.mode("append").saveAsTable(T_LOG)

    done += len(ok)
    print(f"Batch {start // BATCH_SIZE + 1}: {len(ok)} ok, {len(batch) - len(ok)} Fehler | gesamt {done}/{len(todo)}")

print(f"Fertig: {done} Tiles verarbeitet, {len(failed)} Fehler")
for name, err in failed[:20]:
    print(" ✗", name, err)

# COMMAND ----------

# MAGIC %md ## Kontrolle

# COMMAND ----------

display(spark.sql(f"""
SELECT
  (SELECT count(*) FROM {T_TILES})                         AS tiles,
  (SELECT count(*) FROM {T_TERRAIN})                       AS cells,
  (SELECT round(min(elev_min)) FROM {T_TILES})             AS elev_min,
  (SELECT round(max(elev_max)) FROM {T_TILES})             AS elev_max,
  (SELECT count(*) FROM {T_TERRAIN} WHERE share_slope_30_45 > 0.5) AS cells_steep_30_45
"""))

# COMMAND ----------

if failed:
    raise RuntimeError(f"{len(failed)} Tiles fehlgeschlagen – nächster Lauf versucht sie erneut.")