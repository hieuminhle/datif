# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung GA4 via Funnel - Export
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC - Die täglich von der Funnel API abgezogenen Daten der GA4 Daten wurden in der DZ unter dem Schema 02_cleaned für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Tabellen täglich im 04-powerBi-Mart in der PZ-UK updated.
# MAGIC Getriggerde wird das notebook über die pipeline '04-1200-Power-BI-Mart_GA4'.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC   - 03_transformed.ga4_sessions_current_view
# MAGIC   - 03_transformed.ga4_users_current_view
# MAGIC   - 03_transformed.ga4_eco_journal_sessions_total_view
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC   - 04_power_bi_mart.ga4_eco_journal_users_sessions_daily
# MAGIC   - 04_power_bi_mart.ga4_eco_journal_users_sessions_total
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC   - 04.09.2025 Minh Hieu Le: Löschen von Codezellen, die einzelnen Tabellen schreiben
# MAGIC   - 16.07.2025 Justin Stange-Heiduk: Add datif_pz_uk_{env}.
# MAGIC   - 25.06.2025 Justin Stange-Heiduk: Add EcoJournal Daily consolidate
# MAGIC   - 03.06.2025 Justin Stange-Heiduk: Add Strategische Themen
# MAGIC   - 29.04.2025 Justin Stange-Heiduk: Add Abstract
# MAGIC   - 21.02.2025 Svenja Schuder: Init

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

pz_target_schema_name = "04_power_bi_mart"
target_path = "funnel"

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 Users Session x ECO Journal daily

# COMMAND ----------

# load data from PZ-03-transformed into dataframe
df_ga4_eco_users_sessions_daily = spark.sql(f"""
                              SELECT

                                Created_Date,

                                Date,
                                Full_page_URL AS URL,
                                Page_path_GA4 as Page_path,
                                coalesce(Sessions, 0) AS Sessions,
                                coalesce(Views, 0) AS Views,
                                coalesce(User_engagement, 0) AS User_engagement,
                                -- coalesce(Total_session_duration, 0) AS Total_session_duration,
                                -- coalesce(Bounce_rate, 0) AS Bounce_rate,
                                -- coalesce(Average_session_duration, 0) AS Average_session_duration,
                                -- coalesce(Views_per_session, 0) AS Views_per_session,
                                coalesce(Active_Users, 0) AS Active_Users,
                                coalesce(Total_Users, 0) AS Total_Users,
                                Strategische_Themen
                              FROM datif_pz_uk_{env}.03_transformed.ga4_eco_journal_users_sessions_daily_view
                            """)

df_ga4_eco_users_sessions_daily = df_ga4_eco_users_sessions_daily.withColumn("PostType", F.lit("Artikel"))                    

fn_overwrite_table(df_source=df_ga4_eco_users_sessions_daily, target_schema_name=pz_target_schema_name, target_table_name="ga4_eco_journal_users_sessions_daily", target_path=target_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### GA4 Users and Sessions x ECO Journal total

# COMMAND ----------

# load data from PZ-03-transformed into dataframe
df_ga4_eco_users_users_session_total = spark.sql(f"""
                              SELECT
                                Created_Date,                                    
                                Page_path_GA4 AS Page_path,
                                -- Page_path_query_string,
                                -- Page_path_query_string_and_screen_class,
                                Full_page_URL,
                                Engaged_sessions,
                                Sessions,
                                Views,
                                User_engagement,
                                -- Total_session_duration,
                                -- Bounce_rate,
                                -- Average_session_duration,
                                -- Views_per_session,
                                -- Samples_read_rate,
                                Active_Users,
                                Total_Users,
                                Active_Users_per_View,
                                Strategische_Themen,
                                Themenbereich1,
                                Themenbereich2,
                                Themenbereich3,
                                Abstract
                              FROM datif_pz_uk_{env}.03_transformed.ga4_eco_journal_users_sessions_total_view
                            """)

df_ga4_eco_users_users_session_total = df_ga4_eco_users_users_session_total.withColumn("PostType", F.lit("Artikel"))

#fn_overwrite_table(df_source=df_ga4_eco_users_users_session_total, target_schema_name=pz_target_schema_name, target_table_name="ga4_eco_journal_users_sessions_total", target_path=target_path)       

# COMMAND ----------

# MAGIC %md
# MAGIC
