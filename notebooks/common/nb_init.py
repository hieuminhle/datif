# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

secretScope = "keyvault"

# COMMAND ----------

def get_secret(secret_name: str) -> str:
    return dbutils.secrets.get(scope=secretScope,key=secret_name)

# COMMAND ----------

def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        return False

# COMMAND ----------

dz_storage_name = get_secret("storage-datalake-name-dz")
pz_storage_name = get_secret("storage-datalake-name")


sta_endpoint_dz_uk = {
    "raw": f"abfss://01-raw-uk@{dz_storage_name}.dfs.core.windows.net",
    "cleaned": f"abfss://02-cleaned-uk@{dz_storage_name}.dfs.core.windows.net",
    "derived": f"abfss://03-derived-uk@{dz_storage_name}.dfs.core.windows.net"
}

sta_endpoint_pz_uk = {
    "01_raw": f"abfss://01-raw@{pz_storage_name}.dfs.core.windows.net",
    "02_cleaned": f"abfss://02-cleaned@{pz_storage_name}.dfs.core.windows.net",
    "03_transformed": f"abfss://03-transformed@{pz_storage_name}.dfs.core.windows.net",
    "04_icc_mart": f"abfss://04-icc-mart@{pz_storage_name}.dfs.core.windows.net",
    "04_power_bi_mart": f"abfss://04-power-bi-mart@{pz_storage_name}.dfs.core.windows.net",
    "04_ai": f"abfss://04-ai@{pz_storage_name}.dfs.core.windows.net",
    "04_to_ai_assistant": f"abfss://04-to-ai-assistant@{pz_storage_name}.dfs.core.windows.net",
}

# COMMAND ----------

UC_CATALOG = get_secret('dbw-pz-catalog')

# COMMAND ----------

#Unitiy Catalog DaTIF Product Zone UK
import re

env = re.search("(?<=datifpzuk)(.*?)(?=stdfs)", pz_storage_name).group(0)
try:
    spark.sql(f"USE CATALOG datif_pz_uk_{env}")
except Exception as err:
    #catching exception for no isolation shared clusters
    if "org.apache.spark.sql.connector.catalog.CatalogNotFoundException" in str(err):
        print("Unity catalog does not exsist - proceeding with hive metastore catalog")
    else:
        raise err

# COMMAND ----------

def fn_overwrite_table(df_source, target_schema_name, target_table_name, target_path):
    """
    Overwrites a table if it exist or created a new table based on the source DataFrame.
    Args:
        df_source (DataFrame): Source DataFrame.
        target_schema_name (str): Target schema name.
        target_table_name (str): Target table name.
        target_path (str): Path to target.
    Returns:
        None
    """
    full_tablename = f"{target_schema_name}.{target_table_name}"
    target_path = f"{sta_endpoint_pz_uk[target_schema_name]}/{target_path}/{target_table_name}"

    df_source.write.option("overwriteSchema", "true").saveAsTable(
        full_tablename,
        format="delta",
        path=target_path,
        mode="overwrite",
    )

    print(f"Newly created {full_tablename} at storage location: {target_path}")

# COMMAND ----------

def fn_overwrite_csv(df_source, target_schema_name, target_folder_name, filename=None):
    """
    Overwrites CSV output at the given path in Azure Storage (ABFSS).
    
    Args:
        df_source (DataFrame): The DataFrame to write.
        target_schema_name (str): Key in sta_endpoint_pz_uk dict (e.g. '04_ai').
        target_folder_name (str): Subfolder inside container (e.g. 'consolidated_socials').
        filename (str): Optional, filename. If set, the output will be a single file (e.g. "export.csv").
    
    Returns:
        None
    """
    base_path = sta_endpoint_pz_uk[target_schema_name]
    full_path = f"{base_path}/{target_folder_name}"

    if filename:
        full_path = f"{full_path}/{filename}"
    
    # Save as CSV, coalescing to 1 partition if single file is needed
    df_source.coalesce(1).write.option("header", "true").csv(full_path, mode="overwrite")

