"""
swissALTI3D (2 m, GeoTIFF) -> Google Drive -> Databricks Job

Ablauf
------
1. URL-Liste (CSV von swisstopo, eine URL pro Zeile) lesen
2. optional nach Gebiet (LV95-km-Bounding-Box) und Anzahl filtern
3. Tiles, die schon in Drive `data_lake/raw/swissalti3d/` liegen, überspringen
4. Restliche Tiles herunterladen und nach Drive hochladen (in Batches, parallel)
5. Speicherkontingent laufend prüfen und vor dem Volllaufen stoppen
6. Databricks Job `swissalti3d_to_bronze` starten

Secrets / Blocks (Prefect Cloud) – dieselben wie slf_imis.py
-------------------------------------------------------------
- Secret `gdrive-token`            authorized_user-JSON (client_id, client_secret, refresh_token)
- DatabricksCredentials `databricks`
- Root-Ordner: GDRIVE_ROOT_FOLDER_ID (fest im Code), Tiles landen in <root>/swissalti3d/

Deploy
------
uvx prefect-cloud deploy flows/swissalti3d_to_gdrive.py:swissalti3d_to_gdrive \
    --from kevinforter/prefect_workspace \
    --with google-api-python-client --with google-auth --with httpx --with prefect-databricks
"""

from __future__ import annotations

import json
import re
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import httpx
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE

SOURCE_NAME = "swissalti3d"                 # Unterordner im Drive-Root-Ordner
GDRIVE_ROOT_FOLDER_ID = "1anLG5HmPHSO1jknvM-iNTbQMNeQvXp1B"  # wie slf_imis.py / gdrive_upload.py
GDRIVE_TOKEN_BLOCK = "gdrive-token"         # Prefect Secret-Block (authorized_user-JSON)
GDRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
DATABRICKS_BLOCK = "databricks"             # DatabricksCredentials-Block
FOLDER_MIME = "application/vnd.google-apps.folder"
DEFAULT_URLS_CSV = Path(__file__).parent / "data" / "swissalti3d_urls.csv"

# swissalti3d_2019_2501-1120_2_2056_5728.tif
#             Jahr  E-km N-km Aufl. CRS  Höhen-CRS
TILE_RE = re.compile(
    r"swissalti3d_(?P<year>\d{4})_(?P<x>\d{4})-(?P<y>\d{4})_(?P<res>[\d.]+)_2056_\d+\.tif$"
)


# --------------------------------------------------------------------------- #
# Hilfsfunktionen
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Tile:
    url: str
    name: str
    year: int
    x_km: int
    y_km: int
    resolution: str


def parse_tile(url: str) -> Tile | None:
    name = url.rsplit("/", 1)[-1]
    m = TILE_RE.search(name)
    if not m:
        return None
    return Tile(
        url=url,
        name=name,
        year=int(m["year"]),
        x_km=int(m["x"]),
        y_km=int(m["y"]),
        resolution=m["res"],
    )


def resolve_urls_csv(urls_csv: str | None) -> Path | str:
    """Leer/None -> mitgelieferte Liste neben dem Flow. Relative Pfade -> relativ zum Flow."""
    if not urls_csv or not str(urls_csv).strip():
        return DEFAULT_URLS_CSV
    urls_csv = str(urls_csv).strip()
    if urls_csv.startswith(("http://", "https://")):
        return urls_csv
    path = Path(urls_csv)
    if path.exists():
        return path
    candidate = Path(__file__).parent / urls_csv
    if candidate.exists():
        return candidate
    raise FileNotFoundError(
        f"URL-Liste nicht gefunden: {urls_csv} (auch nicht unter {candidate}). "
        "Liegt flows/data/swissalti3d_urls.csv im Repo?"
    )


def read_tile_list(urls_csv: str | None) -> list[Tile]:
    """Liest die swisstopo-CSV (lokaler Pfad oder http(s)-URL)."""
    src = resolve_urls_csv(urls_csv)
    if isinstance(src, str):
        text = httpx.get(src, timeout=60, follow_redirects=True).text
    else:
        text = src.read_text(encoding="utf-8")

    tiles, seen = [], set()
    for line in text.splitlines():
        url = line.strip().strip('"')
        if not url.startswith("http"):
            continue
        tile = parse_tile(url)
        if tile and tile.name not in seen:
            seen.add(tile.name)
            tiles.append(tile)
    return tiles


def filter_tiles(
    tiles: list[Tile], bbox_lv95_km: list[int] | None, max_tiles: int | None
) -> list[Tile]:
    if bbox_lv95_km:
        x_min, y_min, x_max, y_max = bbox_lv95_km
        tiles = [
            t for t in tiles if x_min <= t.x_km <= x_max and y_min <= t.y_km <= y_max
        ]
    # Reihenfolge stabil (West->Ost, Süd->Nord), damit Teil-Läufe nachvollziehbar sind
    tiles = sorted(tiles, key=lambda t: (t.x_km, t.y_km))
    if max_tiles:
        tiles = tiles[:max_tiles]
    return tiles


def _with_retries(fn, *, attempts: int = 5, base_delay: float = 2.0):
    """Wiederholt bei Netzwerkfehlern, 429 und Drive-Rate-Limits (403 rate*)."""
    for i in range(attempts):
        try:
            return fn()
        except HttpError as e:
            status = e.resp.status
            reason = str(e)
            retryable = status in (429, 500, 502, 503, 504) or (
                status == 403 and "ate" in reason and "imit" in reason
            )
            if not retryable or i == attempts - 1:
                raise
        except (httpx.TransportError, httpx.HTTPStatusError, OSError):
            if i == attempts - 1:
                raise
        time.sleep(base_delay * 2**i)


# --------------------------------------------------------------------------- #
# Google Drive
# --------------------------------------------------------------------------- #
_local = threading.local()


def _token_info() -> dict:
    if not hasattr(_token_info, "_cache"):
        value = Secret.load(GDRIVE_TOKEN_BLOCK).get()
        # Prefect liefert JSON-Secrets als dict, Text-Secrets als str
        _token_info._cache = value if isinstance(value, dict) else json.loads(value)
    return _token_info._cache


def drive_service():
    """Ein Drive-Client pro Thread (httplib2 ist nicht thread-safe)."""
    if not hasattr(_local, "drive"):
        creds = Credentials.from_authorized_user_info(_token_info(), GDRIVE_SCOPES)
        _local.drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _local.drive


def get_or_create_folder(name: str, parent_id: str) -> str:
    drive = drive_service()
    q = (
        f"name = '{name}' and '{parent_id}' in parents "
        f"and mimeType = '{FOLDER_MIME}' and trashed = false"
    )
    res = drive.files().list(q=q, fields="files(id)", pageSize=1).execute()
    if res["files"]:
        return res["files"][0]["id"]
    meta = {"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id]}
    return drive.files().create(body=meta, fields="id").execute()["id"]


def list_existing_names(folder_id: str) -> set[str]:
    drive = drive_service()
    names, token = set(), None
    while True:
        res = _with_retries(
            lambda: drive.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="nextPageToken, files(name)",
                pageSize=1000,
                pageToken=token,
            )
            .execute()
        )
        names.update(f["name"] for f in res.get("files", []))
        token = res.get("nextPageToken")
        if not token:
            return names


def drive_free_bytes() -> int | None:
    quota = drive_service().about().get(fields="storageQuota").execute()["storageQuota"]
    if "limit" not in quota:  # unbegrenzt
        return None
    return int(quota["limit"]) - int(quota["usage"])


# --------------------------------------------------------------------------- #
# Speicher-Budget (gemeinsam über alle Threads)
# --------------------------------------------------------------------------- #
class Budget:
    def __init__(self, free_bytes: int | None, reserve_bytes: int):
        self._lock = threading.Lock()
        self._left = None if free_bytes is None else free_bytes - reserve_bytes
        self.exhausted = False

    def reserve(self, n: int) -> bool:
        with self._lock:
            if self._left is None:
                return True
            if self.exhausted or n > self._left:
                self.exhausted = True
                return False
            self._left -= n
            return True


# --------------------------------------------------------------------------- #
# Tasks
# --------------------------------------------------------------------------- #
def _transfer_one(tile: Tile, folder_id: str, tmp_dir: Path, budget: Budget) -> str:
    """Lädt ein Tile herunter und nach Drive hoch. Rückgabe: 'ok' | 'quota' | Fehlertext."""
    if budget.exhausted:
        return "quota"
    local = tmp_dir / tile.name
    try:

        def download():
            with httpx.stream("GET", tile.url, timeout=120, follow_redirects=True) as r:
                r.raise_for_status()
                with open(local, "wb") as fh:
                    for chunk in r.iter_bytes(1 << 20):
                        fh.write(chunk)

        _with_retries(download)

        with open(local, "rb") as fh:
            magic = fh.read(4)
        if magic not in (b"II*\x00", b"MM\x00*", b"II+\x00", b"MM\x00+"):
            return f"kein GeoTIFF ({magic!r})"

        if not budget.reserve(local.stat().st_size):
            return "quota"

        meta = {
            "name": tile.name,
            "parents": [folder_id],
            "description": tile.url,
            "appProperties": {
                "source": SOURCE_NAME,
                "year": str(tile.year),
                "x_km": str(tile.x_km),
                "y_km": str(tile.y_km),
                "resolution_m": tile.resolution,
            },
        }
        media = MediaFileUpload(str(local), mimetype="image/tiff", resumable=False)
        _with_retries(
            lambda: drive_service()
            .files()
            .create(body=meta, media_body=media, fields="id")
            .execute()
        )
        return "ok"
    except Exception as e:  # noqa: BLE001 – einzelne Tiles dürfen scheitern
        return f"{type(e).__name__}: {e}"[:300]
    finally:
        local.unlink(missing_ok=True)


@task(name="upload-tile-batch", retries=1, retry_delay_seconds=30, cache_policy=NO_CACHE)
def upload_batch(
    batch: list[Tile], folder_id: str, max_workers: int, budget: Budget
) -> dict:
    tmp_dir = Path(tempfile.mkdtemp(prefix="swissalti3d_"))
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = list(
                pool.map(lambda t: _transfer_one(t, folder_id, tmp_dir, budget), batch)
            )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    summary = {"ok": 0, "quota": 0, "failed": []}
    for tile, res in zip(batch, results):
        if res in ("ok", "quota"):
            summary[res] += 1
        else:
            summary["failed"].append({"tile": tile.name, "error": res})
    return summary


@task(name="trigger-databricks-job")
def trigger_databricks_job(block_name: str, job_id: int, params: dict) -> int:
    from prefect_databricks import DatabricksCredentials

    creds = DatabricksCredentials.load(block_name)
    host = creds.databricks_instance.removeprefix("https://").rstrip("/")
    resp = httpx.post(
        f"https://{host}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {creds.token.get_secret_value()}"},
        json={"job_id": job_id, "notebook_params": params},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["run_id"]


# --------------------------------------------------------------------------- #
# Flow
# --------------------------------------------------------------------------- #
@flow(name="swissalti3d-to-gdrive", log_prints=True)
def swissalti3d_to_gdrive(
    urls_csv: str | None = None,
    bbox_lv95_km: list[int] | None = None,
    max_tiles: int | None = None,
    batch_size: int = 200,
    max_workers: int = 8,
    drive_reserve_gb: float = 1.0,
    trigger_job: bool = True,
    databricks_job_id: int | None = None,
    databricks_block: str = DATABRICKS_BLOCK,
) -> dict:
    """
    urls_csv:     leer lassen = flows/data/swissalti3d_urls.csv aus dem Repo;
                  sonst relativer Pfad, absoluter Pfad oder http(s)-URL
    bbox_lv95_km: [E_min, N_min, E_max, N_max] in km (untere linke Tile-Ecke),
                  z.B. ganz grob Graubünden: [2695, 1117, 2835, 1213]
    max_tiles:    Obergrenze pro Lauf (gut zum Testen, z.B. 20)
    drive_reserve_gb: so viel Drive-Speicher bleibt mindestens frei
    """
    log = get_run_logger()

    # 1) Tiles bestimmen
    tiles = filter_tiles(read_tile_list(urls_csv), bbox_lv95_km, None)
    log.info("%d Tiles in Auswahl (bbox=%s)", len(tiles), bbox_lv95_km)

    folder_id = get_or_create_folder(SOURCE_NAME, GDRIVE_ROOT_FOLDER_ID)
    existing = list_existing_names(folder_id)
    missing = [t for t in tiles if t.name not in existing]
    already = len(tiles) - len(missing)
    todo = missing[:max_tiles] if max_tiles else missing
    log.info("%d bereits in Drive, %d werden jetzt geladen", already, len(todo))

    # 2) Speicherkontingent
    free = drive_free_bytes()
    if free is not None:
        log.info("Drive frei: %.2f GB (Reserve %.1f GB)", free / 1e9, drive_reserve_gb)
    budget = Budget(free, int(drive_reserve_gb * 1e9))

    # 3) Upload in Batches
    total = {"ok": 0, "quota": 0, "failed": []}
    for i in range(0, len(todo), batch_size):
        if budget.exhausted:
            break
        batch = todo[i : i + batch_size]
        res = upload_batch.with_options(name=f"upload-batch-{i // batch_size + 1:04d}")(
            batch, folder_id, max_workers, budget
        )
        total["ok"] += res["ok"]
        total["quota"] += res["quota"]
        total["failed"] += res["failed"]
        log.info(
            "Batch %d: %d ok, %d Fehler | gesamt %d/%d",
            i // batch_size + 1, res["ok"], len(res["failed"]), total["ok"], len(todo),
        )

    skipped_quota = len(todo) - total["ok"] - len(total["failed"])
    if budget.exhausted:
        log.warning(
            "Drive-Kontingent erreicht: %d Tiles nicht geladen. "
            "Gebiet mit bbox_lv95_km eingrenzen oder Speicher freimachen.",
            skipped_quota,
        )
    for f in total["failed"][:20]:
        log.warning("Fehler %s: %s", f["tile"], f["error"])

    # 4) Databricks Job
    run_id = None
    if trigger_job and databricks_job_id and total["ok"] > 0:
        run_id = trigger_databricks_job(
            databricks_block,
            databricks_job_id,
            {"source_folder_id": folder_id, "full_refresh": "false"},
        )
        log.info("Databricks Job %s gestartet, run_id=%s", databricks_job_id, run_id)
    elif trigger_job and not databricks_job_id:
        log.info("Kein databricks_job_id gesetzt – Job wird nicht gestartet.")

    summary = {
        "selected": len(tiles),
        "already_in_drive": already,
        "uploaded": total["ok"],
        "failed": len(total["failed"]),
        "not_loaded_quota": skipped_quota if budget.exhausted else 0,
        "drive_folder_id": folder_id,
        "databricks_run_id": run_id,
    }
    log.info("Zusammenfassung: %s", summary)

    if total["failed"]:
        raise RuntimeError(
            f"{len(total['failed'])} Tiles fehlgeschlagen – beim nächsten Lauf "
            "werden sie automatisch erneut versucht."
        )
    return summary


if __name__ == "__main__":
    # Kleiner Testlauf: 5 Tiles, ohne Databricks Job
    swissalti3d_to_gdrive(bbox_lv95_km=[2780, 1180, 2790, 1190], max_tiles=5, trigger_job=False)