# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung GA4 via Funnel - Daily Export
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC - Die täglich von der Funnel API abgezogenen Daten der GA4 Daten wurden in der DZ unter dem Schema 03_transformed für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Tabellen täglich im 04-icc-mart in der PZ-UK updated.
# MAGIC Getriggerde wird das notebook über die pipeline '04-1200-icc-Mart_GA4'.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC - 03_transformed.ga4_sessions_current_view
# MAGIC - 03_transformed.ga4_users_current_view
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 04_icc_mart.ga4_sessions_daily
# MAGIC - 04_icc_mart.ga4_users_daily
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 21.02.2025 Max Mustermann: Init

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

pz_target_schema_name = "04_icc_mart"
target_path = "funnel"

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 - Sessions

# COMMAND ----------

# load data from PZ-03-transformed into dataframe
df_ga4_sessions_daily = spark.sql("""
                              SELECT
                                    Date,
                                    Session_campaign,
                                    First_user_campaign,                                    
                                    Page_path_GA4 AS Page_path,
                                    Page_path_query_string,
                                    Page_path_query_string_and_screen_class,
                                    Full_page_URL,
                                    Session_source__medium,
                                    Engaged_sessions,
                                    Sessions,
                                    Views,
                                    User_engagement,
                                    Total_session_duration,
                                    Bounce_rate,
                                    Average_session_duration,
                                    Views_per_session
                                FROM 03_transformed.ga4_eco_journal_sessions_view
                              """)

fn_overwrite_table(df_source=df_ga4_sessions_daily, target_schema_name=pz_target_schema_name, target_table_name="ga4_eco_journal_sessions_daily", target_path=target_path)      

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 - Users

# COMMAND ----------

# load data from PZ-03-transformed into dataframe
df_ga4_users_daily = spark.sql("""
                              SELECT
                                Date,
                                Page_path_GA4 AS Page_path,
                                Page_path_query_string,
                                Page_path_query_string_and_screen_class,
                                Full_page_URL,
                                Samples_read_rate,
                                Session_campaign,
                                Session_source__medium,
                                Active_Users,
                                Total_Users
                              FROM 03_transformed.ga4_eco_journal_users_view
                            """)

fn_overwrite_table(df_source=df_ga4_users_daily, target_schema_name=pz_target_schema_name, target_table_name="ga4_eco_journal_users_daily", target_path=target_path)      

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 Sessions x ECO Journal
# MAGIC

# COMMAND ----------

# # load data from PZ-03-transformed into dataframe
# df_ga4_eco_sessions_daily = spark.sql("""
#                               SELECT
#                                 Date,
#                                 Session_campaign,
#                                 First_user_campaign,                                    
#                                 Page_path_GA4 AS Page_path,
#                                 Page_path_query_string,
#                                 Page_path_query_string_and_screen_class,
#                                 Full_page_URL,
#                                 Session_source__medium,
#                                 Engaged_sessions,
#                                 Sessions,
#                                 Views,
#                                 User_engagement,
#                                 Total_session_duration,
#                                 Bounce_rate,
#                                 Average_session_duration,
#                                 Views_per_session,
#                                 Themenbereich1,
#                                 Themenbereich2,
#                                 Themenbereich3
#                               FROM 03_transformed.ga4_eco_journal_sessions_view
#                             """)

# #display(df_ga4_eco_sessions_daily)
# fn_overwrite_table(df_source=df_ga4_eco_sessions_daily, target_schema_name=pz_target_schema_name, target_table_name="ga4_eco_journal_sessions_daily", target_path=target_path)         

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 Users x ECO Journal

# COMMAND ----------

# # load data from PZ-03-transformed into dataframe
# df_ga4_eco_users_daily = spark.sql("""
#                               SELECT
#                                 Date,
#                                 Page_path_GA4 as Page_path,
#                                 Page_path_query_string,
#                                 Page_path_query_string_and_screen_class,
#                                 Full_page_URL,
#                                 Samples_read_rate,
#                                 Session_campaign,
#                                 Session_source__medium,
#                                 Active_Users,
#                                 Total_Users,
#                                 Themenbereich1,
#                                 Themenbereich2,
#                                 Themenbereich3
#                               FROM 03_transformed.ga4_eco_journal_users_view
#                             """)

# fn_overwrite_table(df_source=df_ga4_eco_users_daily, target_schema_name=pz_target_schema_name, target_table_name="ga4_eco_journal_users_daily", target_path=target_path)       
