# Databricks notebook source
# Importiere Spark-Sitzung
from pyspark.sql import SparkSession

# Starte eine Spark-Sitzung (wird in Databricks bereits ausgeführt)
spark = SparkSession.builder.getOrCreate()

# Definiere Quell- und Zieltabellen
source_catalog = "datif_dz_dev"
source_schema = "03_derived_uk_ai"
source_table = "consolidated_socials"

target_catalog = "datif_pz_uk_dev"
target_schema = "03_served"
target_table = "consolidated_socials"

# SQL-Abfrage zur Erstellung einer tiefen Kopie (Deep Clone)
clone_query = f"""
CREATE TABLE {target_catalog}.{target_schema}.{target_table} 
CLONE {source_catalog}.{source_schema}.{source_table};
"""

# Führe die Abfrage aus, um die Tabelle zu klonen
spark.sql(clone_query)
