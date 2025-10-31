# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Argus Medien historischer Abzug
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die historischen Daten der Argus Media Panels wurden in der DZ unter dem Schema 02_cleaned_uk_argus für die PZ-UK bereitgestellt.
# MAGIC Im Cleaned Schema der DZ wurden bereits bereinigte Daten bereitgestellt. Mit diesem notebook werden die Tabellen einmal initial im 03-transformed schema der PZ geladen.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_dz_dev.02_cleaned_uk_argus.argus_online_media_panel_historic
# MAGIC - datif_dz_dev.02_cleaned_uk_argus.argus_online_media_panel_current_view
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_dev.03_transformed.argus_online_media_panel
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):     
# MAGIC - 08.10.2024 Svenja Schuder: init

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

dz_storage_name = get_secret("storage-datalake-name-dz")
pz_storage_name = get_secret("storage-datalake-name")


sta_endpoint_dz_uk = {
    "raw": f"abfss://01-raw-uk@{dz_storage_name}.dfs.core.windows.net",
    "cleaned": f"abfss://02-cleaned-uk@{dz_storage_name}.dfs.core.windows.net",
    "derived": f"abfss://03-derived-uk@{dz_storage_name}.dfs.core.windows.net"
}

sta_endpoint_pz_uk = {
    "raw": f"abfss://01-raw@{pz_storage_name}.dfs.core.windows.net",
    "cleaned": f"abfss://02-cleaned@{pz_storage_name}.dfs.core.windows.net",
    "transformed": f"abfss://03-transformed@{pz_storage_name}.dfs.core.windows.net",
    "icc-mart": f"abfss://04-icc-mart@{pz_storage_name}.dfs.core.windows.net",
    "powerbi-mart": f"abfss://04-power-bi-mart@{pz_storage_name}.dfs.core.windows.net"
}

pz_target_schema_name = "datif_pz_uk_dev.03_transformed"
target_path = "argus"

# COMMAND ----------

def fn_build_merge_condition(base:str, source:str, keys: list, conjunction: str = "and"):
  """
  Builds merge-conditions based on and source and target alias name and a list of keys.
  The Keys must be present in both the source and the target.
  """

  connect = f" {conjunction.strip()} "
  conditions = [f"{base}.{key} <=> {source}.{key}" for key in keys]

  res = connect.join(conditions)

  return res

# COMMAND ----------

def fn_create_or_upsert_table(df_source, target_path, target_database_name, target_table_name, primary_key):
    """
    Creates a table if it does not exist or upserts a table based on the source DataFrame.
    Args:
        df_source (DataFrame): Source DataFrame.
        target_table_name (str): Target table name.
        target_database_name (str): Target database name.
        target_path (str): Path to target.
        primary_key (list): List of primary key column names.
    Returns:
        None
    """
    full_tablename = f"{target_database_name}.{target_table_name}"
    target_path = f"{sta_endpoint_pz_uk['transformed']}/{target_path}/{target_table_name}"
    table_exists = spark.sql(f"SHOW TABLES IN {target_database_name} LIKE '{target_table_name}'").count()

    # If the table does not exist, create it
    if not table_exists:
        df_source.write.saveAsTable(
            full_tablename,
            format="delta",
            path=target_path,
            mode="overwrite",
        )
        print(f"Newly created {full_tablename}") 
    # If the table exists, upsert the data
    else:
        merge_condition = fn_build_merge_condition("target", "source", primary_key)
        delta_target_table = DeltaTable.forName(spark, full_tablename)
        delta_target_table.alias("target") \
            .merge(df_source.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"Updated {full_tablename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Online Media

# COMMAND ----------

# load data from DZ-02_cleaned into dataframe
df_online_daily = spark.sql("SELECT * FROM datif_dz_dev.02_cleaned_uk_argus.argus_online_media_panel_current_view")

# COMMAND ----------

# load data from DZ-02_cleaned into dataframe
df_online_historic = spark.sql("SELECT * FROM datif_dz_dev.02_cleaned_uk_argus.argus_online_media_panel_historic")

# COMMAND ----------

df_online_combined = df_online_historic.union(df_online_daily)
print(df_online_combined.count())

# COMMAND ----------

# Upsert dataframe into derived table
fn_create_or_upsert_table(df_source=df_online_combined,target_path=target_path,target_database_name=pz_target_schema_name,target_table_name="argus_online_media_panel",primary_key=["clipping_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Media

# COMMAND ----------

# load data from DZ-02_cleaned into dataframe
df_print_daily = spark.sql("SELECT * FROM datif_dz_dev.02_cleaned_uk_argus.argus_print_media_panel_current_view")

# COMMAND ----------

# load data from DZ-02_cleaned into dataframe
df_print_historic = spark.sql("SELECT * FROM datif_dz_dev.02_cleaned_uk_argus.argus_print_media_panel_historic")

# COMMAND ----------

df_print_combined = df_print_historic.union(df_print_daily)
print(df_print_combined.count())

# COMMAND ----------

# Upsert dataframe into derived table
fn_create_or_upsert_table(df_source=df_print_combined,target_path=target_path,target_database_name=pz_target_schema_name,target_table_name="argus_print_media_panel",primary_key=["clipping_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Social Listening 

# COMMAND ----------

# load data from DZ-02_cleaned into dataframe
df_social_daily = spark.sql("SELECT * FROM datif_dz_dev.02_cleaned_uk_argus.argus_social_listening_panel_current_view")

# COMMAND ----------

print(df_social_daily.count())

# COMMAND ----------

# Upsert dataframe into derived table
fn_create_or_upsert_table(df_source=df_social_daily,target_path=target_path,target_database_name=pz_target_schema_name,target_table_name="argus_social_listening_panel",primary_key=["clipping_id"])
