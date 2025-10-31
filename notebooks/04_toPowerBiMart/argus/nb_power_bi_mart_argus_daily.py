# Databricks notebook source
# MAGIC %md
# MAGIC # 04-PowerBI-Mart: Datenbereitstellung Argus Medien Panel
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der API abgezogenen Daten des Argus Media Panels wurden in der DZ unter dem Schema 02_cleaned_uk_argus für die PZ-UK bereitgestellt.
# MAGIC In der PZ wurden die Argus Datenabzüge unter 03_transformed angereichert.
# MAGIC Mit diesem notebook werden die Tabellen täglich im 04_icc_mart schema der PZ updated.
# MAGIC Getriggerde wird das notebook über die pipeline '00-1000-Argus-orchestrate'.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_dev.03_transformed.argus_online_media_panel
# MAGIC - datif_pz_uk_dev.03_transformed.argus_print_media_panel
# MAGIC - datif_pz_uk_dev.03_transformed.argus_social_listening_panel
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_dev.04_power_bi_mart.argus_online_media_panel
# MAGIC - datif_pz_uk_dev.04_power_bi_mart.argus_print_media_panel
# MAGIC - datif_pz_uk_dev.04_power_bi_mart.argus_social_listening_panel
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 15.11.2024 Svenja Schuder: update to overwrite method
# MAGIC - 15.10.2024 Svenja Schuder: init
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Parameters

# COMMAND ----------

pz_target_schema_name = "04_power_bi_mart"
target_path = "argus"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## Online Media

# COMMAND ----------

df_online_media_transformed = spark.sql(f"""
                                   SELECT 
                                   clipping_id,
                                   clipping_publication_date,
                                   clipping_deeplink,
                                   clipping_headline,
                                   clipping_subtitle,
                                   clipping_search_terms,
                                   clipping_teaser,
                                   clipping_abstract,
                                   clipping_order,
                                   clipping_rating,
                                   clipping_author,
                                   media_title,
                                   media_editorial,
                                   media_publisher,
                                   media_genre,
                                   media_type,
                                   media_sector,
                                   media_state,
                                   media_gross_reach,
                                   media_visits,
                                   media_page_views,
                                   media_unique_user
                                   FROM datif_pz_uk_dev.03_transformed.argus_online_media_panel
                                   """)

# overwrite table in power-bi-mart
fn_overwrite_table(df_source=df_online_media_transformed, target_schema_name=pz_target_schema_name, target_table_name="argus_online_media_panel", target_path=target_path)                                 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Media

# COMMAND ----------

df_print_media_transformed = spark.sql(f"""
                                   SELECT 
                                   clipping_id,
                                   clipping_publication_date,
                                   clipping_deeplink,
                                   clipping_headline,
                                   clipping_subtitle,
                                   clipping_search_terms,
                                   clipping_teaser,
                                   clipping_abstract,
                                   clipping_order,
                                   clipping_rating,
                                   clipping_author,
                                   media_title,
                                   media_editorial,
                                   media_publisher,
                                   media_genre,
                                   media_type,
                                   media_sector,
                                   media_state,
                                   media_gross_reach,
                                   media_print_run,
                                   media_copies_sold,
                                   media_copies_distributed,
                                   files_pdf_doc
                                   FROM datif_pz_uk_dev.03_transformed.argus_print_media_panel
                                   """)

# overwrite table in power-bi-mart
fn_overwrite_table(df_source=df_print_media_transformed, target_schema_name=pz_target_schema_name, target_table_name="argus_print_media_panel", target_path=target_path)                                    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Social Listening

# COMMAND ----------

# load data from DZ-02_cleaned into dataframe
df_social_transformed = spark.sql("""
                            SELECT clipping_id,
                            clipping_publication_date,
                            clipping_deeplink,
                            clipping_headline,
                            clipping_subtitle,
                            clipping_search_terms,
                            clipping_teaser,
                            clipping_rating,
                            clipping_author,
                            clipping_language,
                            media_title,
                            media_editorial,
                            media_publisher,
                            media_genre,
                            media_type,
                            media_sector,
                            medium_url,
                            media_gross_reach
                            FROM datif_pz_uk_dev.03_transformed.argus_social_listening_panel
                            """)

# overwrite table in power-bi-mart                            
fn_overwrite_table(df_source=df_social_transformed, target_schema_name=pz_target_schema_name, target_table_name="argus_social_listening_panel", target_path=target_path)
