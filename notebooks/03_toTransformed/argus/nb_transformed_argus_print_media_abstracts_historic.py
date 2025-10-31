# Databricks notebook source
# MAGIC %md
# MAGIC # Argus - Print Media Panel - JOIN Abstract nach 03_transformed.argus_print_media_panel
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):  
# MAGIC In diesem Notebook werden die per LLM generierten Abstracts für die Print Medien Artikel, in der Tabelle hinzugefügt. 
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - 02-cleaned/argus/print_media_abstracts/YYYY/MM/DD/binary/{clipping_id}.parquet
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - datif_pz_uk_dev.03_transformed.argus_print_media_panel
# MAGIC   
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):     
# MAGIC * 16.10.2024 Svenja Schuder: Init
# MAGIC
# MAGIC
# MAGIC

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
# MAGIC ## 2.1 Widgets

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

# MAGIC %md
# MAGIC ## 2.2 Parameters

# COMMAND ----------

pz_target_schema_name = "datif_pz_uk_dev.03_transformed"
target_path = "argus"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Functions

# COMMAND ----------

def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        return False

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
# MAGIC ## 2.4 Data Schemas

# COMMAND ----------

from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Define the schema for JSON file
schema_print = StructType([
    StructField("clipping_id", StringType(), True),
    StructField("Text", StringType(), True)
])

# COMMAND ----------

# Define the schema for df_abstract
schema_abstract = StructType([
    StructField("clipping_id", StringType(), nullable=False),
    StructField("Abstract", StringType(), nullable=False),
    StructField("Tonalität", StringType(), nullable=False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

df_print_abstracts_cleaned_historic = spark.sql(f"""
                                   SELECT *
                                   FROM datif_pz_uk_dev.02_cleaned.argus_print_media_abstracts_historic
                                   WHERE clipping_id Between '42_5887071' AND '42_6297121'
                                   """)

# Create a temporary view of df_print_media_cleaned_table
df_print_abstracts_cleaned_historic.createOrReplaceTempView("print_abstracts_cleaned")

# COMMAND ----------

df_print_abstracts_cleaned_historic.display()

# COMMAND ----------

df_print_media_cleaned = spark.sql(f"""
                                   SELECT *
                                   FROM datif_pz_uk_dev.03_transformed.argus_print_media_panel
                                   WHERE clipping_id Between '42_5887071' AND '42_6297121'
                                   """)

# Create a temporary view of df_print_media_cleaned_table
df_print_media_cleaned.createOrReplaceTempView("print_media_cleaned")

# COMMAND ----------

# Perform a left anti join to get records that are not in delta table print_media_cleaned
df_print_media_union = spark.sql("""
    SELECT a.clipping_id, 
    b.clipping_publication_date,
    b.clipping_deeplink,
    b.clipping_headline,
    b.clipping_subtitle,
    b.clipping_search_terms,
    b.clipping_teaser,
    a.Abstract as clipping_abstract,
    b.clipping_order,
    a.Tonalitaet as clipping_rating,
    b.clipping_author,
    b.media_title,
    b.media_editorial,
    b.media_publisher,
    b.media_genre,
    b.media_type,
    b.media_sector,
    b.media_state,
    b.media_gross_reach,
    b.media_print_run,
    b.media_copies_sold,
    b.media_copies_distributed,
    b.files_pdf_doc,
    b.KEY_HASH,
    b.RECORD_HASH,
    b.PARTITION_HASH,
    b.LOAD_TS,
    b.DELETED_TS,
    b.OPERATION    
    FROM print_abstracts_cleaned a
    JOIN print_media_cleaned b
    ON a.clipping_id = b.clipping_id
""")

# COMMAND ----------

# # Create a DataFrame for the current abstract and sentiment
# target_df = spark.createDataFrame([], df_print_media_union.schema)

# # Append the current DataFrame to df_abstract
# target_df = target_df.union(df_print_media_union)
# display(target_df)

# COMMAND ----------

# Upsert dataframe into derived table
fn_create_or_upsert_table(df_source=df_print_media_union,target_path=target_path,target_database_name=pz_target_schema_name,target_table_name="argus_print_media_panel",primary_key=["clipping_id"])
