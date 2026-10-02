"""
MeteoSchweiz OGD (SwissMetNet) -> Databricks Unity Catalog Volume  |  Landing

Lädt die Stations-CSVs der automatischen Wetterstationen (Collection
ch.meteoschweiz.ogd-smn) über die STAC-API und legt sie unverändert in einem
Databricks Volume ab:

    <volume_root>/ogd-smn/<station>/ogd-smn_<station>_<g>_<period>.csv
    <volume_root>/ogd-smn/_meta/ogd-smn_meta_*.csv
    <volume_root>/_state/ogd-smn_manifest.json      (Checksummen für Inkrement)

Inkrementell: Die STAC-Checksumme jeder hochgeladenen Datei steht im Manifest.
Unveränderte Dateien werden übersprungen, geänderte überschrieben.

Zugangsdaten: Prefect-Block vom Typ "Databricks Credentials" mit Namen
"databricks-credentials" (Felder: Databricks Instance + Token).

Aufruf lokal (optional):
    python meteoswiss_to_volume.py                         # alle Stationen
    python meteoswiss_to_volume.py --stations chz lug      # nur bestimmte Stationen

Quelle: MeteoSchweiz (Quellenangabe ist Lizenzbedingung).
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect_databricks import DatabricksCredentials

# --------------------------------------------------------------------------- #
# Konfiguration
# --------------------------------------------------------------------------- #
STAC_BASE = "https://data.geo.admin.ch/api/stac/v1"
COLLECTION = "ch.meteoschweiz.ogd-smn"
DATASET = "ogd-smn"

CREDENTIALS_BLOCK = "databricks-credentials"
DEFAULT_VOLUME_ROOT = "/Volumes/workspace/meteoswiss/landing"

DEFAULT_STATIONS = ["all"]                     # alle Stationen; z.B. ["chz", "lug"]
DEFAULT_GRANULARITIES = ["h", "d", "m", "y"]   # t = 10-Minuten-Werte (gross!)
DEFAULT_PERIODS = ["historical", "recent"]     # "now" = seit Mitternacht
DEFAULT_MIN_YEAR = 1990

HTTP_TIMEOUT = httpx.Timeout(120.0, connect=15.0)
USER_AGENT = "kev-weather-warehouse/0.1 (private, non-commercial)"

# Dateiname: ogd-smn_<station>_<g>[_<period>[_<yyyy-yyyy>]].csv
ASSET_RE = re.compile(
    r"^ogd-smn_(?P<station>[a-z0-9]+)_(?P<gran>[tdhmy])"
    r"(?:_(?P<period>historical|recent|now))?"
    r"(?:_(?P<start>\d{4})-(?P<end>\d{4}))?\.csv$"
)


@dataclass(frozen=True)
class Asset:
    key: str              # Dateiname, z.B. ogd-smn_chz_h_recent.csv
    href: str
    checksum: str | None  # STAC file:checksum (Multihash, "1220" + sha256-hex)
    updated: str | None
    folder: str           # Unterordner (Station oder "_meta")


def _normalize_list(value: list[str] | str | None) -> list[str]:
    """Macht Parameter robust gegen UI-Eingaben wie '["chz"]', 'chz, lug' oder 'chz'."""
    if value is None:
        return []
    items = [value] if isinstance(value, str) else list(value)
    out: list[str] = []
    for item in items:
        text = str(item).strip()
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                out.extend(str(p) for p in (parsed if isinstance(parsed, list) else [parsed]))
                continue
            except json.JSONDecodeError:
                text = text.strip("[]")
        out.extend(p for p in re.split(r"[,\s]+", text.replace('"', "").replace("'", "")) if p)
    return [p.strip().lower() for p in out if p.strip()]


# --------------------------------------------------------------------------- #
# STAC
# --------------------------------------------------------------------------- #
def _http() -> httpx.Client:
    return httpx.Client(
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    )


def _to_asset(key: str, meta: dict, folder: str) -> Asset:
    return Asset(
        key=key,
        href=meta["href"],
        checksum=meta.get("file:checksum"),
        updated=meta.get("updated"),
        folder=folder,
    )


@task(retries=3, retry_delay_seconds=[10, 30, 90], cache_policy=NO_CACHE)
def list_all_stations() -> list[str]:
    """Alle Station-IDs der Collection (paginiert über den next-Link)."""
    ids: list[str] = []
    url: str | None = f"{STAC_BASE}/collections/{COLLECTION}/items?limit=100"
    with _http() as client:
        while url:
            r = client.get(url)
            r.raise_for_status()
            payload = r.json()
            ids.extend(f["id"] for f in payload.get("features", []))
            url = next(
                (l["href"] for l in payload.get("links", []) if l.get("rel") == "next"),
                None,
            )
    return sorted(ids)


@task(retries=3, retry_delay_seconds=[10, 30, 90], cache_policy=NO_CACHE)
def list_station_assets(
    station: str, granularities: list[str], periods: list[str], min_year: int
) -> list[Asset]:
    """Assets einer Station, gefiltert nach Granularität, Periode und Jahr."""
    with _http() as client:
        r = client.get(f"{STAC_BASE}/collections/{COLLECTION}/items/{station}")
        if r.status_code == 404:
            get_run_logger().warning("Station %s existiert nicht – übersprungen", station)
            return []
        r.raise_for_status()
        assets = r.json().get("assets", {})

    selected: list[Asset] = []
    for key, meta in assets.items():
        m = ASSET_RE.match(key)
        if not m or m["gran"] not in granularities:
            continue
        # m/y-Dateien haben keine Periode -> immer nehmen, falls Granularität gewählt
        if m["period"] and m["period"] not in periods:
            continue
        # min_year wirkt nur auf Dateien mit Jahrzehnt im Namen (h/t historical)
        if m["end"] and int(m["end"]) < min_year:
            continue
        selected.append(_to_asset(key, meta, folder=station))
    return sorted(selected, key=lambda a: a.key)


@task(retries=3, retry_delay_seconds=[10, 30, 90], cache_policy=NO_CACHE)
def list_collection_metadata() -> list[Asset]:
    """Collection-Assets: Stationsliste, Parameterbeschreibung, Dateninventar."""
    with _http() as client:
        r = client.get(f"{STAC_BASE}/collections/{COLLECTION}")
        r.raise_for_status()
        assets = r.json().get("assets", {})
    return sorted(
        (_to_asset(k, v, folder="_meta") for k, v in assets.items() if k.endswith(".csv")),
        key=lambda a: a.key,
    )


# --------------------------------------------------------------------------- #
# Databricks Volume
# --------------------------------------------------------------------------- #
_cred_lock = threading.Lock()
_creds: dict | None = None
_local = threading.local()


def _load_credentials() -> dict:
    """Host + Token aus dem Prefect-Block 'DatabricksCredentials'."""
    try:
        block = DatabricksCredentials.load(CREDENTIALS_BLOCK)
    except ValueError as exc:
        raise RuntimeError(
            f"Prefect-Block '{CREDENTIALS_BLOCK}' vom Typ 'Databricks Credentials' fehlt "
            "in diesem Workspace."
        ) from exc
    if block.token is None:
        raise RuntimeError(f"Im Block '{CREDENTIALS_BLOCK}' ist kein Token gesetzt.")
    instance = block.databricks_instance.strip().rstrip("/")
    host = instance if instance.startswith("http") else f"https://{instance}"
    return {"host": host, "token": block.token.get_secret_value()}


def _workspace() -> WorkspaceClient:
    """Ein WorkspaceClient pro Thread; Zugangsdaten aus dem Prefect-Block."""
    global _creds
    with _cred_lock:
        if _creds is None:
            _creds = _load_credentials()
    if getattr(_local, "ws", None) is None:
        _local.ws = WorkspaceClient(host=_creds["host"], token=_creds["token"])
    return _local.ws


def _manifest_path(volume_root: str) -> str:
    return f"{volume_root}/_state/{DATASET}_manifest.json"


@task(retries=2, retry_delay_seconds=15, cache_policy=NO_CACHE)
def load_manifest(volume_root: str) -> dict[str, dict]:
    try:
        resp = _workspace().files.download(_manifest_path(volume_root))
        return json.loads(resp.contents.read())
    except NotFound:
        return {}


@task(retries=3, retry_delay_seconds=[10, 30, 60], cache_policy=NO_CACHE)
def save_manifest(volume_root: str, manifest: dict[str, dict]) -> None:
    data = json.dumps(manifest, indent=1, sort_keys=True).encode()
    _workspace().files.upload(_manifest_path(volume_root), io.BytesIO(data), overwrite=True)


@task(retries=2, retry_delay_seconds=15, cache_policy=NO_CACHE)
def ensure_directories(volume_root: str, folders: list[str]) -> None:
    """Legt die Zielordner an (idempotent)."""
    ws = _workspace()
    ws.files.create_directory(f"{volume_root}/_state")
    for folder in folders:
        ws.files.create_directory(f"{volume_root}/{DATASET}/{folder}")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@task(
    retries=3,
    retry_delay_seconds=[15, 60, 180],
    cache_policy=NO_CACHE,
    task_run_name="sync-{asset.key}",
)
def sync_asset(asset: Asset, target: str, verify: bool = True) -> dict:
    """Lädt ein Asset von MeteoSchweiz und schreibt es ins Volume."""
    log = get_run_logger()
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / asset.key
        with _http() as client, client.stream("GET", asset.href) as r:
            r.raise_for_status()
            with local.open("wb") as f:
                for chunk in r.iter_bytes(1 << 20):
                    f.write(chunk)

        # file:checksum ist ein Multihash: 0x12 (sha2-256) 0x20 (32 Byte) + Digest
        if verify and asset.checksum and asset.checksum.startswith("1220"):
            actual = _sha256(local)
            if actual != asset.checksum[4:]:
                raise ValueError(
                    f"Checksumme stimmt nicht für {asset.key} "
                    f"(erwartet {asset.checksum[4:12]}…, erhalten {actual[:8]}…)"
                )

        size = local.stat().st_size
        with local.open("rb") as f:
            _workspace().files.upload(target, f, overwrite=True)

    log.info("uploaded %s (%.1f MB)", asset.key, size / 1e6)
    return {
        "checksum": asset.checksum,
        "source_updated": asset.updated,
        "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "bytes": size,
    }


# --------------------------------------------------------------------------- #
# Flow
# --------------------------------------------------------------------------- #
@flow(name="meteoswiss-smn-to-volume")
def meteoswiss_to_volume(
    stations: list[str] = DEFAULT_STATIONS,
    granularities: list[str] = DEFAULT_GRANULARITIES,
    periods: list[str] = DEFAULT_PERIODS,
    min_year: int = DEFAULT_MIN_YEAR,
    include_metadata: bool = True,
    volume_root: str = DEFAULT_VOLUME_ROOT,
    force: bool = False,
    verify_checksum: bool = True,
) -> dict[str, int]:
    log = get_run_logger()
    volume_root = volume_root.rstrip("/")

    stations = _normalize_list(stations)
    granularities = _normalize_list(granularities)
    periods = _normalize_list(periods)

    station_ids = list_all_stations() if stations in ([], ["all"]) else stations
    log.info("%d Stationen: %s", len(station_ids), ", ".join(station_ids[:10]))

    assets: list[Asset] = []
    for sid in station_ids:
        assets.extend(list_station_assets(sid, granularities, periods, min_year))
    if include_metadata:
        assets.extend(list_collection_metadata())

    manifest = load_manifest(volume_root)
    ensure_directories(volume_root, sorted({a.folder for a in assets}))

    todo: list[tuple[Asset, str]] = []
    skipped = 0
    for a in assets:
        rel = f"{DATASET}/{a.folder}/{a.key}"
        known = manifest.get(rel, {}).get("checksum")
        if not force and a.checksum and known == a.checksum:
            skipped += 1
        else:
            todo.append((a, rel))
    log.info("%d Assets total, %d unverändert, %d zu laden", len(assets), skipped, len(todo))

    futures = [
        (rel, sync_asset.submit(a, f"{volume_root}/{rel}", verify=verify_checksum))
        for a, rel in todo
    ]

    loaded = failed = 0
    for rel, fut in futures:
        try:
            manifest[rel] = fut.result()
            loaded += 1
        except Exception as exc:  # einzelne Datei soll den Lauf nicht abbrechen
            failed += 1
            log.error("Fehlgeschlagen %s: %s", rel, exc)

    # Manifest immer schreiben, damit erfolgreiche Uploads beim nächsten Lauf zählen
    if loaded:
        save_manifest(volume_root, manifest)

    summary = {"loaded": loaded, "skipped": skipped, "failed": failed}
    log.info("Zusammenfassung: %s", summary)
    if failed:
        raise RuntimeError(f"{failed} Asset(s) fehlgeschlagen: {summary}")
    return summary


# --------------------------------------------------------------------------- #
# CLI (lokaler Test)
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--stations", nargs="+", default=DEFAULT_STATIONS)
    p.add_argument("--granularities", nargs="+", default=DEFAULT_GRANULARITIES)
    p.add_argument("--periods", nargs="+", default=DEFAULT_PERIODS)
    p.add_argument("--min-year", type=int, default=DEFAULT_MIN_YEAR)
    p.add_argument("--volume-root", default=DEFAULT_VOLUME_ROOT)
    p.add_argument("--no-metadata", action="store_true")
    p.add_argument("--force", action="store_true", help="alles neu hochladen")
    a = p.parse_args()
    meteoswiss_to_volume(
        stations=a.stations,
        granularities=a.granularities,
        periods=a.periods,
        min_year=a.min_year,
        include_metadata=not a.no_metadata,
        volume_root=a.volume_root,
        force=a.force,
    )
