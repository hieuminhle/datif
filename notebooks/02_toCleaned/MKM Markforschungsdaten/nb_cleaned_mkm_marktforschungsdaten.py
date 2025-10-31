# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Anbindung Eco Journal Themen 
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC - Die UK bekommt Marktforschungsdaten von Mindline (Reputationsdaten, Markenbekanntheit etc), diese liegen im Blob Storage als Xlsx und sollen hier in 02_cleaned
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Data Storage: 
# MAGIC - PZ-UK-01-Raw: MKM Marktforschungsdaten/
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_gestuetzte_markenbekanntheit(_bawue)
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_reputationsindex(_bawue)
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_ungestuetzte_enbw-themen(_bawue)
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_ungestuetzte_energie-themen(_bawue)
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_ungestuetzte_markenbekanntheit(_bawue)
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 12.06.2025 Justin Stange-Heiduk: init

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

import os
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import lit, collect_list, concat_ws, regexp_replace, to_date, coalesce, date_format, col
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql
from pyspark.sql import DataFrame as SparkDF

# COMMAND ----------

# Define schema name and path to store tables
target_schema_name = "02_cleaned"
target_path = "mkm_marktforschungsdaten"

# Set source and trg path
source_path = sta_endpoint_pz_uk["01_raw"] + "/MKM Marktforschungsdaten"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Excel File and do a dataframe

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd
import os

# Dictionary zum Speichern der DataFrames
df_dict = {}
# Dateien aus dem Path
file_info_list = dbutils.fs.ls(source_path)

for file_info in file_info_list:
    filename = file_info.name
    if filename.endswith('.csv'):
        filepath = os.path.join(source_path, filename)
        try:
            # Read the CSV file using Spark
            spark_csv = (spark.read.format("csv")
                         .option("inferSchema", "true")
                         .option("header", "true")
                         .option("delimiter", ";")
                         .option("treatEmptyValuesAsNulls", "true")
                         .load(filepath))

            print(f"Lade Datei {filename}...")
            df_cleaned = spark_csv.na.fill(0)

            # Speichern im Dictionary
            df_dict[filename.replace(".csv", "")] = df_cleaned

            # Optional: Variable im globalen Scope
            globals()[f"df_{filename.replace('.csv', '').replace(' ', '_')}"] = df_cleaned

        except Exception as e:
            print(f" Fehler bei {filename}: {e}")

# COMMAND ----------

def fn_overwrite_table_exec(target_schema_name: str, target_path: str) -> None:
    """
    Speichert alle df_mkm_* Spark DataFrames als Tabellen in einem angegebenen Schema und Pfad.

    Args:
        target_schema_name (str): Ziel-Schema.
        target_path (str): Ziel-Pfad im Data Storage
    """

    for var_name in list(globals()):
        if var_name.startswith("df_mkm_"):
            df = globals()[var_name]
            if isinstance(df, pyspark.sql.DataFrame):
                table_name = var_name.replace("df_", "").replace("-", "_").lower()

                # Tabelle löschen, falls bereits vorhanden
                spark.sql(f"DROP TABLE IF EXISTS {target_schema_name}.{table_name}")
                print(f"DataFrame '{var_name}' enthält folgende Spalten:")
                df.printSchema()
                # Tabelle neu schreiben
                fn_overwrite_table(
                    df_source=df,
                    target_schema_name=target_schema_name,
                    target_table_name=table_name,
                    target_path=target_path
                )
                print(f"✅ Tabelle '{table_name}' wurde überschrieben.")
            else:
                print(f"⚠️ Variable {var_name} ist kein Spark DataFrame. Wird übersprungen.")

fn_overwrite_table_exec(target_schema_name, target_path)
