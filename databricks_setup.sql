-- ========================================================================== --
-- Databricks-Setup für MeteoSchweiz SwissMetNet
-- Im SQL Editor ausführen (SQL Warehouse). Katalog "workspace" ist der
-- Standardkatalog der Free Edition; bei Bedarf anpassen.
-- ========================================================================== --

-- 1) Einmalig: Schema + Volume für die Rohdateien -------------------------------
CREATE SCHEMA IF NOT EXISTS workspace.meteoswiss
  COMMENT 'Quelle: MeteoSchweiz Open Data (SwissMetNet)';

CREATE VOLUME IF NOT EXISTS workspace.meteoswiss.landing
  COMMENT 'Unveränderte CSVs aus dem Prefect-Flow meteoswiss_to_volume';


-- 2) Nach dem ersten Flow-Lauf: kurz reinschauen --------------------------------
LIST '/Volumes/workspace/meteoswiss/landing/ogd-smn/chz/';

SELECT *
FROM read_files(
  '/Volumes/workspace/meteoswiss/landing/ogd-smn/chz/ogd-smn_chz_d_recent.csv',
  format => 'csv', sep => ';', header => true
)
LIMIT 20;


-- 3) Bronze-Tabellen ------------------------------------------------------------
-- Rohdaten 1:1 als Text + Herkunftsdatei. Typisieren/Deduplizieren passiert in
-- Silver. historical ändert selten, recent täglich -> beide werden hier
-- komplett neu aufgebaut (bei diesen Volumen unproblematisch).

-- Tageswerte
CREATE OR REPLACE TABLE workspace.meteoswiss.bronze_smn_daily AS
SELECT *, _metadata.file_path AS source_file, current_timestamp() AS _loaded_at
FROM read_files(
  '/Volumes/workspace/meteoswiss/landing/ogd-smn/*/ogd-smn_*_d_*.csv',
  format => 'csv', sep => ';', header => true,
  inferSchema => false           -- alles als STRING, Bronze bleibt verlustfrei
);
-- Hinweis: Stationen messen nicht alle dieselben Parameter. Spalten, die nicht
-- im abgeleiteten Schema sind, landen in der Spalte _rescued_data (JSON).

-- Stundenwerte (gross: beim ersten Mal ein paar Minuten auf 2X-Small)
CREATE OR REPLACE TABLE workspace.meteoswiss.bronze_smn_hourly AS
SELECT *, _metadata.file_path AS source_file, current_timestamp() AS _loaded_at
FROM read_files(
  '/Volumes/workspace/meteoswiss/landing/ogd-smn/*/ogd-smn_*_h_*.csv',
  format => 'csv', sep => ';', header => true,
  inferSchema => false
);

-- Stations-Metadaten (Umlaute: Encoding prüfen, MeteoSchweiz nutzt hier
-- meines Wissens Windows-1252)
CREATE OR REPLACE TABLE workspace.meteoswiss.bronze_smn_meta_stations AS
SELECT *
FROM read_files(
  '/Volumes/workspace/meteoswiss/landing/ogd-smn/_meta/ogd-smn_meta_stations.csv',
  format => 'csv', sep => ';', header => true, encoding => 'windows-1252'
);
