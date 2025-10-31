# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Daily
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der Funnel API abgezogenen Daten der Social Media Daten wurden in der DZ unter dem Schema 02_cleaned für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggerde wird das notebook über die pipeline '00-1000-Funnel-orchestrate'.
# MAGIC
# MAGIC ---
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 03_transformed.facebook_organic_scd2_view
# MAGIC - 03_transformed.instagram_organic_scd2_view
# MAGIC - 03_transformed.instagram_organic_stories_scd2_view
# MAGIC - 03_transformed.x_organic_daily_view
# MAGIC - 03_transformed.linkedin_organic_post_daily_view
# MAGIC - 03_transformed.linkedin_organic_video_daily_view
# MAGIC
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 04_power_bi_mart.consolidated_socials_daily
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 25.09.2025 Minh Hieu Le: Sicherstellen, dass in den Zahlenspalten keine Nullwerte stehen, sondern stattdessen Nullen
# MAGIC - 04.09.2025 Minh Hieu Le: Löschen von Codezellen, die einzelnen Tabellen für jedes Kanal schreiben
# MAGIC - 16.07.2025 Max Mustermann: Add datif_pz_uk_{env}.
# MAGIC - 25.06.2025 Max Mustermann: Youtube Owner Spalte hinzugefügt
# MAGIC - 11.06.2025 Max Mustermann: SoMe Daily Consolidated hinzugefügt
# MAGIC - 12.05.2025 Max Mustermann: LinkedIn Consolidated hinzugefügt
# MAGIC - 07.05.2025 Max Mustermann: Instagram API Änderung Impression -> Views
# MAGIC - 06.05.2025 Minh Hieu Le: Bei Youtube VideoURL mit selektiert
# MAGIC - 18.03.2025 Max Mustermann: Diff Spalte für Meta hinzugefügt
# MAGIC - 13.02.2025 Max Mustermann: Schema Updates
# MAGIC - 06.02.2025 Max Mustermann: Unity Catalog als Variable hinzugefügt
# MAGIC - 03.12.2024 Max Mustermann: Schemabereinigungen
# MAGIC - 16.12.2024 Max Mustermann: update ig stories table
# MAGIC - 28.11.2024 Max Mustermann: Schemaänderung + Upsert Methode zu Overwrite geändert + instagram_stories und youtube hinzugefügt
# MAGIC - 13.11.2024 Max Mustermann: init
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Parameters

# COMMAND ----------

pz_target_schema_name = "04_power_bi_mart"
target_path = "funnel"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04-power-bi-mart

# COMMAND ----------

# MAGIC %md
# MAGIC ### SoMe Consolidate Daily

# COMMAND ----------

# load data from PZ-03-transformed into dataframe
df_consolidate_daily = (
    spark.table(f"datif_pz_uk_{env}.03_transformed.consolidated_socials_daily")
    .withColumn("Impressions", F.coalesce(F.col("Impressions"), F.lit(0))) \
    .withColumn("EngagementRate", F.coalesce(F.col("EngagementRate"), F.lit(0))) \
    .withColumn("WeightedEngagement", F.coalesce(F.col("WeightedEngagement"), F.lit(0))) \
    .withColumn("Engagement", F.coalesce(F.col("Engagement"), F.lit(0))) \
)

fn_overwrite_table(df_source=df_consolidate_daily, target_schema_name=pz_target_schema_name, target_table_name="consolidated_socials_daily", target_path=target_path)  
