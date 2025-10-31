# Databricks notebook source
# MAGIC %md
# MAGIC # 03-Transformed: Datenbereitstellung Argus Online Medien Panel & Social Listening
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der API abgezogenen Daten des Argus Media Panels wurden in der DZ unter dem Schema 02_cleaned_uk_argus für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggerde wird das notebook über die pipeline '00-1000-Argus-orchestrate'.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_dz_dev.02_cleaned_uk_argus.argus_online_media_panel_current_view
# MAGIC - datif_dz_dev.02_cleaned_uk_argus.argus_social_listening_panel_current_view
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_dev.03_transformed.argus_online_media_panel
# MAGIC - datif_pz_uk_dev.03_transformed.argus_social_listening_panel
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):     
# MAGIC - 15.11.2024 Svenja Schuder: Code refactoring
# MAGIC - 16.10.2024 Svenja Schuder: Init

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Parameters

# COMMAND ----------

# Create a text input field for the entry date
dbutils.widgets.text("p_date", "yyyy/MM/dd HH:mm:ss")

# Get Entry Date from ADF Pipeline
p_date = dbutils.widgets.get("p_date")
date = p_date.split(" ")[0]
date_path = date.replace("/", "_")
query_date = date.replace("/", "-")
time_part = p_date.split(" ")[1]
time = time_part.replace(":", "_")

# COMMAND ----------

pz_target_schema_name = "datif_pz_uk_dev.03_transformed"
target_path = "argus"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Functions

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
    target_path = f"{sta_endpoint_pz_uk['03_transformed']}/{target_path}/{target_table_name}"
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
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Online Media

# COMMAND ----------

# load data from DZ-02_cleaned into dataframe
df_online_daily = spark.sql(f"""
                            SELECT * 
                            FROM datif_dz_dev.02_cleaned_uk_argus.argus_online_media_panel_current_view
                            WHERE LOAD_TS LIKE '{query_date}%' 
                            """)

# COMMAND ----------

# Upsert dataframe into derived table
fn_create_or_upsert_table(df_source=df_online_daily,target_path=target_path,target_database_name=pz_target_schema_name,target_table_name="argus_online_media_panel",primary_key=["clipping_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Social Listening 

# COMMAND ----------

# load data from DZ-02_cleaned into dataframe
df_social_daily = spark.sql(f"""
                            SELECT * 
                            FROM datif_dz_dev.02_cleaned_uk_argus.argus_social_listening_panel_current_view
                            WHERE LOAD_TS LIKE '{query_date}%' 
                            """)


# COMMAND ----------

# Upsert dataframe into derived table
fn_create_or_upsert_table(df_source=df_social_daily,target_path=target_path,target_database_name=pz_target_schema_name,target_table_name="argus_social_listening_panel",primary_key=["clipping_id"])
