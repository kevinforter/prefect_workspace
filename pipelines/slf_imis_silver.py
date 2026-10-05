# Databricks notebook source
# MAGIC %md
# MAGIC # SLF IMIS – Bronze → Silver
# MAGIC
# MAGIC Liest die Bronze-Tabellen aus `r2_to_bronze` (Archiv + Live), typisiert sie und behält pro
# MAGIC `station_code` + `measure_date` den zuletzt geladenen Wert. Schreibt Silver komplett neu (overwrite).
# MAGIC Fehlt eine Bronze-Tabelle noch (z.B. Archiv nie geladen), wird sie übersprungen.

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

dbutils.widgets.text("schema", "workspace.slf")
SCHEMA = dbutils.widgets.get("schema")

MEAS_COLS = [
    "hs", "ta_30min_mean", "rh_30min_mean", "tss_30min_mean", "ts0_30min_mean",
    "ts25_30min_mean", "ts50_30min_mean", "ts100_30min_mean", "rswr_30min_mean",
    "vw_30min_mean", "vw_30min_max", "dw_30min_mean", "dw_30min_sd",
]
TARGETS = {
    # Silver-Tabelle             -> (Bronze-Quellen, Wertspalten)
    "silver_imis_measurements": (["bronze_imis_history_measurements", "bronze_imis_live_measurements"], MEAS_COLS),
    "silver_imis_precipitation": (["bronze_imis_history_precipitation", "bronze_imis_live_precipitation"], ["rr_10min_sum"]),
    "silver_imis_daily_snow": (["bronze_imis_history_daily_snow", "bronze_imis_live_daily_snow"], ["hs", "hn_1d"]),
}

# COMMAND ----------

def _exists(table: str) -> bool:
    return spark.catalog.tableExists(f"{SCHEMA}.{table}")


def _dbl(col: str):
    return F.expr(f"try_cast(cast(`{col}` AS STRING) AS DOUBLE)")


def _ts(col: str):
    """API: ISO mit Z. Archiv: Format beim ersten Lauf prüfen und ggf. ergänzen."""
    c = F.col(col).cast("string")
    return F.coalesce(*[
        F.try_to_timestamp(c, F.lit(fmt))
        for fmt in (
            "yyyy-MM-dd'T'HH:mm:ssX",
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
            "yyyy-MM-dd HH:mm:ssXXX",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
        )
    ])


def _typed(table: str, value_cols: list[str]) -> DataFrame:
    df = spark.table(f"{SCHEMA}.{table}")
    missing = {"station_code", "measure_date"} - set(df.columns)
    if missing:
        raise ValueError(f"{table}: Spalten {missing} fehlen – vorhanden: {df.columns}")
    return df.select(
        F.upper(F.col("station_code").cast("string")).alias("station_code"),
        _ts("measure_date").alias("measure_date"),
        *[(_dbl(c) if c in df.columns else F.lit(None).cast("double")).alias(c) for c in value_cols],
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def build_silver(target: str, sources: list[str], value_cols: list[str]) -> None:
    parts = [_typed(t, value_cols) for t in sources if _exists(t)]
    if not parts:
        print(f"{target}: keine Bronze-Quelle vorhanden – übersprungen")
        return
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)

    w = Window.partitionBy("station_code", "measure_date").orderBy(F.col("_ingested_at").desc())
    df = (
        df.where(F.col("station_code").isNotNull() & F.col("measure_date").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.{target}")
    print(f"{target}: {spark.table(f'{SCHEMA}.{target}').count():,} Zeilen")

# COMMAND ----------

for target, (sources, cols) in TARGETS.items():
    build_silver(target, sources, cols)

# COMMAND ----------

# Stationen: letzter Snapshot pro Station
if _exists("bronze_imis_live_stations"):
    w = Window.partitionBy("code").orderBy(F.col("_ingested_at").desc())
    (
        spark.table(f"{SCHEMA}.bronze_imis_live_stations")
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .select(
            F.upper("code").alias("station_code"),
            "label",
            _dbl("lon").alias("lon"),
            _dbl("lat").alias("lat"),
            _dbl("elevation").alias("elevation"),
            "country_code",
            "canton_code",
            "type",
            F.col("_ingested_at").alias("_snapshot_at"),
        )
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{SCHEMA}.silver_imis_stations")
    )
    print("silver_imis_stations aktualisiert")
