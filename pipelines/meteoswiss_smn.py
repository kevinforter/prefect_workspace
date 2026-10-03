"""
Lakeflow Declarative Pipeline: MeteoSchweiz SwissMetNet  |  Volume -> Bronze -> Silver

Liest die CSVs, die der Prefect-Flow meteoswiss_to_volume ins Volume legt:
    <volume_root>/ogd-smn/<station>/ogd-smn_<station>_<g>_<period>.csv
    <volume_root>/ogd-smn/_meta/ogd-smn_meta_*.csv

Tabellen pro Granularität (hourly, daily, monthly, yearly):

  bronze_smn_<g>   Streaming-Tabelle, Auto Loader, append-only.
                   Alle Werte als STRING, plus Herkunftsdatei und Zeitstempel.
                   Überschriebene Dateien (v.a. *_recent.csv, täglich) werden
                   erneut eingelesen -> Bronze enthält bewusst Duplikate.

  silver_smn_<g>   AUTO CDC (SCD Typ 1) auf (station_abbr, reference_ts):
                   genau eine Zeile pro Station und Zeitpunkt, die jeweils
                   neueste Lieferung gewinnt. Messwerte als DOUBLE.

Dazu bronze_smn_meta_<name>: Materialized Views über die Metadaten-CSVs.

Pipeline-Konfiguration (Settings -> Configuration):
    meteoswiss.volume_root = /Volumes/workspace/meteoswiss/landing

Quelle: MeteoSchweiz (Quellenangabe ist Lizenzbedingung).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME_ROOT = spark.conf.get(  # noqa: F821  (spark wird von der Pipeline bereitgestellt)
    "meteoswiss.volume_root", "/Volumes/workspace/meteoswiss/landing"
).rstrip("/")
BASE = f"{VOLUME_ROOT}/ogd-smn"

# Kürzel -> Tabellen-Suffix und Datei-Muster.
# h/d haben eine Periode im Namen (…_h_recent.csv), m/y nicht (…_m.csv).
GRANULARITIES = {
    "h": ("hourly", "ogd-smn_*_h_*.csv"),
    "d": ("daily", "ogd-smn_*_d_*.csv"),
    "m": ("monthly", "ogd-smn_*_m.csv"),
    "y": ("yearly", "ogd-smn_*_y.csv"),
}

# Zeitstempel in den OGD-CSVs, z.B. "01.01.2025 00:00" (UTC)
TS_FORMAT = "dd.MM.yyyy HH:mm"

TECH_COLUMNS = {
    "station_abbr",
    "reference_timestamp",
    "source_file",
    "source_modified_at",
    "_ingested_at",
    "_rescued_data",
}

META_FILES = {
    "stations": "ogd-smn_meta_stations.csv",
    "parameters": "ogd-smn_meta_parameters.csv",
    "datainventory": "ogd-smn_meta_datainventory.csv",
}
META_ENCODING = "windows-1252"  # Metadaten enthalten Umlaute


def _define_granularity(code: str, suffix: str, pattern: str) -> None:
    bronze = f"bronze_smn_{suffix}"
    typed = f"smn_{suffix}_typed"
    silver = f"silver_smn_{suffix}"

    # ---------------------------------------------------------------- Bronze
    @dp.table(
        name=bronze,
        comment=f"MeteoSchweiz SwissMetNet ({suffix}), Rohdaten aus dem Volume. "
        "Quelle: MeteoSchweiz.",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("sep", ";")
            .option("cloudFiles.inferColumnTypes", "false")       # alles STRING
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.allowOverwrites", "true")         # *_recent.csv neu einlesen
            .load(f"{BASE}/*/{pattern}")
            .select(
                "*",
                F.col("_metadata.file_path").alias("source_file"),
                F.col("_metadata.file_modification_time").alias("source_modified_at"),
                F.current_timestamp().alias("_ingested_at"),
            )
        )

    # ------------------------------------------------- Typisierung (temporär)
    @dp.temporary_view(name=typed)
    def _typed():
        df = spark.readStream.table(bronze)  # noqa: F821
        values = [
            F.col(f"`{c}`").cast("double").alias(c)
            for c in df.columns
            if c not in TECH_COLUMNS
        ]
        return df.select(
            F.upper(F.trim("station_abbr")).alias("station_abbr"),
            F.to_timestamp("reference_timestamp", TS_FORMAT).alias("reference_ts"),
            *values,
            "source_file",
            "source_modified_at",
        ).where("station_abbr IS NOT NULL AND reference_ts IS NOT NULL")

    # -------------------------------------------------------------- Silver
    dp.create_streaming_table(
        name=silver,
        comment=f"MeteoSchweiz SwissMetNet ({suffix}), eine Zeile pro Station und "
        "Zeitpunkt, neueste Lieferung gewinnt. Quelle: MeteoSchweiz.",
        table_properties={"quality": "silver"},
        expect_all_or_drop={"valid_ts": "reference_ts IS NOT NULL"},
    )
    dp.create_auto_cdc_flow(
        target=silver,
        source=typed,
        keys=["station_abbr", "reference_ts"],
        sequence_by=F.struct("source_modified_at", "source_file"),
        stored_as_scd_type=1,
    )


for _code, (_suffix, _pattern) in GRANULARITIES.items():
    _define_granularity(_code, _suffix, _pattern)


# ------------------------------------------------------------------ Metadaten
def _define_meta(name: str, file_name: str) -> None:
    @dp.materialized_view(
        name=f"bronze_smn_meta_{name}",
        comment=f"MeteoSchweiz SwissMetNet Metadaten ({name}). Quelle: MeteoSchweiz.",
    )
    def _meta():
        return (
            spark.read.format("csv")  # noqa: F821
            .option("header", "true")
            .option("sep", ";")
            .option("encoding", META_ENCODING)
            .load(f"{BASE}/_meta/{file_name}")
            .withColumn("source_file", F.col("_metadata.file_path"))
        )


for _name, _file in META_FILES.items():
    _define_meta(_name, _file)
