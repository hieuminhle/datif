# Databricks notebook source
# MAGIC %md
# MAGIC # 03-Transformed: Datenbereitstellung Genios Medien Panel
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der Genios API abgezogenen Daten wurden in der DZ unter dem Schema 02_cleaned_uk_genios für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggerde wird das notebook über die pipeline 'tbd'.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_dz_dev.02_cleaned_uk_genios.genios_A_online_media_panel_current_view
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_dev.03_transformed.genios_A_online_media_panel
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 27.01.2025 Max Mustermann: Init

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %run ../../common/nb_tagging_functions

# COMMAND ----------

from openai import AzureOpenAI
from typing import List, Dict
import re
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import time
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# DBTITLE 1,Parameters

target_schema_name = "03_transformed"
target_path = "genios"
limit_n = 1 # limit number of newly generated abstracts per load (the 'limit_n' number of newest published articles are used)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# DBTITLE 1,Online Media
df_genios_online = spark.read.table("datif_dz_dev.02_cleaned_uk_genios.genios_A_online_media_panel_current_view")

df_genios_online = df_genios_online.select(
    F.col("docId").alias("DocID"),
    F.col("title").alias("Title"),
    F.col("subTitle").alias("Subtitle"),
    F.col("category").alias("Category"),
    F.col("publishingDay").alias("PublishingDay"),
    F.col("indexedTimestamp").alias("IndexedTimestamp"),
    F.col("source").alias("Source"),
    F.col("database").alias("Database"),
    F.col("geniosWebDocumentUrl").alias("GeniosWebDocumentUrl"),
    F.col("priceNetInCents").alias("PriceNetInCents"),
    F.col("vat").alias("Vat"),
    F.col("priceGrossInCents").alias("PriceGrossInCents"),
    # F.col("fullTextId").alias("FullTextId"),
    # F.col("thumbnailData").alias("ThumbnailData"),
    F.col("content.lead").alias("Lead"),
    F.col("content.text").alias("Text"),
)

df_genios_online = df_genios_online.withColumn("CLevelErwaehnungen", c_level_udf(df_genios_online.Text))

try:
    print('trying to join already computed tags from the transformed layer')
    df_transformed = spark.read.table("datif_pz_uk_dev.03_transformed.genios_A_online_media_panel").alias("transformed")
    df = join_pre_computed(df_genios_online, df_transformed, "DocID", "Strategie2030", ["abstract", "tags"])
    print('successfully joined already computed tags from the transformed layer')
except Exception as e:
    if "cannot be found" in str(e) or "cannot be resolved" in str(e):
        df = select_abstracts_or_none(df_genios_online)
        df = select_tags_or_none(df)
    else:
        raise

df = generate_limited(df, "DocID", "PublishingDay", limit_n, "Abstract", ["abstract", "tags"], "Text")
fn_overwrite_table(df, target_schema_name=target_schema_name, target_table_name="genios_A_online_media_panel", target_path=target_path)
# df.display()


# COMMAND ----------

df_genios_print = spark.read.table("datif_dz_dev.02_cleaned_uk_genios.genios_A_print_media_panel_current_view")

df_genios_print = df_genios_print.select(
    F.col("docId").alias("DocID"),
    F.col("title").alias("Title"),
    F.col("subTitle").alias("Subtitle"),
    F.col("category").alias("Category"),
    F.col("publishingDay").alias("PublishingDay"),
    F.col("indexedTimestamp").alias("IndexedTimestamp"),
    F.col("source").alias("Source"),
    F.col("database").alias("Database"),
    F.col("geniosWebDocumentUrl").alias("GeniosWebDocumentUrl"),
    F.col("priceNetInCents").alias("PriceNetInCents"),
    F.col("vat").alias("Vat"),
    F.col("priceGrossInCents").alias("PriceGrossInCents"),
    # F.col("fullTextId").alias("FullTextId"),
    # F.col("thumbnailData").alias("ThumbnailData"),
    F.col("content.lead").alias("Lead"),
    F.col("content.text").alias("Text"),
)

df_genios_print = df_genios_print.withColumn("CLevelErwaehnungen", c_level_udf(df_genios_print.Text))

try:
    print('trying to join already computed tags from the transformed layer')
    df_transformed = spark.read.table("datif_pz_uk_dev.03_transformed.genios_A_print_media_panel").alias("transformed")
    df = join_pre_computed(df_genios_print, df_transformed, "DocID", "Strategie2030", ["abstract", "tags"])
    print('successfully joined already computed tags from the transformed layer')
except Exception as e:
    if "cannot be found" in str(e) or "cannot be resolved" in str(e):
        df = select_abstracts_or_none(df_genios_print)
        df = select_tags_or_none(df)
    else:
        raise

df = generate_limited(df, "DocID", "PublishingDay", limit_n, "Abstract", ["abstract", "tags"], "Text")
fn_overwrite_table(df, target_schema_name=target_schema_name, target_table_name="genios_A_print_media_panel", target_path=target_path)
# df.display()
