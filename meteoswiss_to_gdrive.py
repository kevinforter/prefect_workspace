"""
MeteoSchweiz OGD (SwissMetNet) -> Google Drive  |  Bronze-Landing

Lädt die Stations-CSVs der automatischen Wetterstationen (Collection
ch.meteoschweiz.ogd-smn) über die STAC-API und legt sie unverändert in
Google Drive ab:

    <drive_root>/bronze/meteoswiss/ogd-smn/<station>/ogd-smn_<station>_<g>_<period>.csv
    <drive_root>/bronze/meteoswiss/ogd-smn/_meta/ogd-smn_meta_*.csv

Inkrementell ohne eigene State-Tabelle: Die STAC-Checksumme jedes Assets wird
als appProperty an der Drive-Datei gespeichert. Unveränderte Dateien werden
übersprungen, geänderte in place aktualisiert (Drive behält die Revisionen).

Aufruf:
    python meteoswiss_to_gdrive.py run                      # einmalig, alle ~160 Stationen
    python meteoswiss_to_gdrive.py run --stations chz lug   # nur bestimmte Stationen
    python meteoswiss_to_gdrive.py serve                    # täglicher Prefect-Schedule

Voraussetzung: einmalig gdrive_auth_setup.py ausführen (legt das OAuth-Token
als Prefect Secret Block "gdrive-oauth-token" ab).

Quelle: MeteoSchweiz (Quellenangabe ist Lizenzbedingung).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path

import httpx
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE

# --------------------------------------------------------------------------- #
# Konfiguration
# --------------------------------------------------------------------------- #
STAC_BASE = "https://data.geo.admin.ch/api/stac/v1"
COLLECTION = "ch.meteoschweiz.ogd-smn"
SOURCE_PATH = ["bronze", "meteoswiss", "ogd-smn"]

TOKEN_BLOCK = "gdrive-oauth-token"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
FOLDER_MIME = "application/vnd.google-apps.folder"

DEFAULT_STATIONS = ["all"]                     # alle Stationen; z.B. ["chz", "lug"] zum Einschränken
DEFAULT_GRANULARITIES = ["h", "d", "m", "y"]   # t = 10-Minuten-Werte (gross!)
DEFAULT_PERIODS = ["historical", "recent"]     # "now" = seit Mitternacht
DEFAULT_MIN_YEAR = 1990

HTTP_TIMEOUT = httpx.Timeout(60.0, connect=15.0)
USER_AGENT = "kev-weather-warehouse/0.1 (private, non-commercial)"

# Dateiname: ogd-smn_<station>_<g>[_<period>[_<yyyy-yyyy>]].csv
ASSET_RE = re.compile(
    r"^ogd-smn_(?P<station>[a-z0-9]+)_(?P<gran>[tdhmy])"
    r"(?:_(?P<period>historical|recent|now))?"
    r"(?:_(?P<start>\d{4})-(?P<end>\d{4}))?\.csv$"
)


@dataclass(frozen=True)
class Asset:
    key: str             # Dateiname, z.B. ogd-smn_chz_h_recent.csv
    href: str
    checksum: str | None  # STAC file:checksum (Multihash, "1220" + sha256-hex)
    updated: str | None
    folder: str          # Unterordner in Drive (Station oder "_meta")


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
    url = f"{STAC_BASE}/collections/{COLLECTION}/items/{station}"
    with _http() as client:
        r = client.get(url)
        if r.status_code == 404:
            get_run_logger().warning("Station %s existiert nicht – übersprungen", station)
            return []
        r.raise_for_status()
        assets = r.json().get("assets", {})

    selected: list[Asset] = []
    for key, meta in assets.items():
        m = ASSET_RE.match(key)
        if not m:
            continue
        if m["gran"] not in granularities:
            continue
        # m/y-Dateien haben keine Periode -> immer nehmen, falls Granularität gewählt
        if m["period"] and m["period"] not in periods:
            continue
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
# Google Drive
# --------------------------------------------------------------------------- #
_cred_lock = threading.Lock()
_token_info: dict | None = None
_local = threading.local()


def _credentials() -> Credentials:
    global _token_info
    with _cred_lock:
        if _token_info is None:
            raw = Secret.load(TOKEN_BLOCK).get()
            _token_info = json.loads(raw) if isinstance(raw, str) else dict(raw)
    creds = Credentials.from_authorized_user_info(_token_info, SCOPES)
    if not creds.valid:
        creds.refresh(Request())
    return creds


def _drive():
    """Ein Drive-Client pro Thread (httplib2 ist nicht thread-safe)."""
    if getattr(_local, "svc", None) is None:
        _local.svc = build("drive", "v3", credentials=_credentials(), cache_discovery=False)
    return _local.svc


def _q(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _find(name: str, parent_id: str, folder: bool = False) -> dict | None:
    q = f"name = '{_q(name)}' and '{parent_id}' in parents and trashed = false"
    if folder:
        q += f" and mimeType = '{FOLDER_MIME}'"
    res = (
        _drive()
        .files()
        .list(q=q, spaces="drive", fields="files(id, name, appProperties)", pageSize=10)
        .execute()
    )
    files = res.get("files", [])
    return files[0] if files else None


def _ensure_folder(name: str, parent_id: str) -> str:
    existing = _find(name, parent_id, folder=True)
    if existing:
        return existing["id"]
    body = {"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id]}
    return _drive().files().create(body=body, fields="id").execute()["id"]


@task(retries=2, retry_delay_seconds=15, cache_policy=NO_CACHE)
def ensure_folders(drive_root_name: str, subfolders: list[str]) -> dict[str, str]:
    """Legt Ordnerpfad + Unterordner sequenziell an (verhindert Duplikate bei Parallelität)."""
    parent = _ensure_folder(drive_root_name, "root")
    for part in SOURCE_PATH:
        parent = _ensure_folder(part, parent)
    return {sub: _ensure_folder(sub, parent) for sub in subfolders}


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
def sync_asset(asset: Asset, folder_id: str, force: bool = False, verify: bool = True) -> str:
    """Lädt ein Asset herunter und schreibt es nach Drive, falls geändert.

    Rückgabe: "skipped" | "created" | "updated"
    """
    log = get_run_logger()
    existing = _find(asset.key, folder_id)
    known = (existing or {}).get("appProperties", {}).get("src_checksum")

    if existing and not force and asset.checksum and known == asset.checksum:
        log.debug("%s unverändert", asset.key)
        return "skipped"

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

        props = {
            "src_checksum": asset.checksum or "",
            "src_updated": asset.updated or "",
            "src_system": "meteoswiss-ogd-smn",
        }
        media = MediaFileUpload(
            str(local), mimetype="text/csv", resumable=True, chunksize=8 * 1024 * 1024
        )
        files = _drive().files()

        if existing:
            files.update(
                fileId=existing["id"], body={"appProperties": props}, media_body=media
            ).execute()
            action = "updated"
        else:
            files.create(
                body={"name": asset.key, "parents": [folder_id], "appProperties": props},
                media_body=media,
                fields="id",
            ).execute()
            action = "created"

        size_mb = local.stat().st_size / 1e6
    log.info("%s %s (%.1f MB)", action, asset.key, size_mb)
    return action


# --------------------------------------------------------------------------- #
# Flow
# --------------------------------------------------------------------------- #
@flow(name="meteoswiss-smn-to-gdrive")
def meteoswiss_to_gdrive(
    stations: list[str] = DEFAULT_STATIONS,
    granularities: list[str] = DEFAULT_GRANULARITIES,
    periods: list[str] = DEFAULT_PERIODS,
    min_year: int = DEFAULT_MIN_YEAR,
    include_metadata: bool = True,
    drive_root_name: str = "data-lake",
    force: bool = False,
    verify_checksum: bool = True,
) -> dict[str, int]:
    log = get_run_logger()

    station_ids = list_all_stations() if stations == ["all"] else [s.lower() for s in stations]
    log.info("Stationen: %s", ", ".join(station_ids))

    assets: list[Asset] = []
    for sid in station_ids:
        assets.extend(list_station_assets(sid, granularities, periods, min_year))
    if include_metadata:
        assets.extend(list_collection_metadata())
    log.info("%d Assets nach Filter", len(assets))

    folders = ensure_folders(drive_root_name, sorted({a.folder for a in assets}))

    futures = [
        sync_asset.submit(a, folders[a.folder], force=force, verify=verify_checksum)
        for a in assets
    ]

    summary = {"created": 0, "updated": 0, "skipped": 0, "failed": 0}
    for fut in futures:
        try:
            summary[fut.result()] += 1
        except Exception as exc:  # einzelne Datei soll nicht den ganzen Lauf abbrechen
            summary["failed"] += 1
            log.error("Fehlgeschlagen: %s", exc)

    log.info("Zusammenfassung: %s", summary)
    if summary["failed"]:
        raise RuntimeError(f"{summary['failed']} Asset(s) fehlgeschlagen: {summary}")
    return summary


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _cli() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Flow einmalig ausführen")
    run.add_argument("--stations", nargs="+", default=DEFAULT_STATIONS)
    run.add_argument("--granularities", nargs="+", default=DEFAULT_GRANULARITIES)
    run.add_argument("--periods", nargs="+", default=DEFAULT_PERIODS)
    run.add_argument("--min-year", type=int, default=DEFAULT_MIN_YEAR)
    run.add_argument("--no-metadata", action="store_true")
    run.add_argument("--force", action="store_true", help="alles neu hochladen")

    srv = sub.add_parser("serve", help="als Prefect-Deployment mit täglichem Schedule bedienen")
    srv.add_argument("--cron", default="5 14 * * *")
    srv.add_argument("--stations", nargs="+", default=DEFAULT_STATIONS)

    args = p.parse_args()

    if args.cmd == "run":
        meteoswiss_to_gdrive(
            stations=args.stations,
            granularities=args.granularities,
            periods=args.periods,
            min_year=args.min_year,
            include_metadata=not args.no_metadata,
            force=args.force,
        )
    else:
        params = {"stations": args.stations}
        try:  # Prefect >= 3.3: Cron mit Zeitzone
            from prefect.schedules import Cron

            meteoswiss_to_gdrive.serve(
                name="daily",
                schedules=[Cron(args.cron, timezone="Europe/Zurich")],
                parameters=params,
            )
        except ImportError:  # ältere Prefect-3-Versionen: Cron in UTC
            meteoswiss_to_gdrive.serve(name="daily", cron=args.cron, parameters=params)


if __name__ == "__main__":
    _cli()
