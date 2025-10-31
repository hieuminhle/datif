# Databricks notebook source
# MAGIC %run ../common/nb_init

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.types as T
import pyspark.sql.functions as F

# COMMAND ----------

relevant_tables = [ # auf die current view schauen
    'datif_pz_uk_sqldb_dev.social.enriched_instagram_organic_contents',
    'datif_pz_uk_sqldb_dev.social.enriched_facebook_organic_contents',
    'datif_pz_uk_sqldb_dev.social.enriched_twitter_organic_contents',
    'datif_pz_uk_sqldb_dev.social.enriched_linkedin_organic_contents',
]
#     'datif_pz_uk_sqldb_dev.web.enriched_ga4_contents',
#     'datif_dz_dev.02_cleaned_uk_argus.argus_online_media_panel',
#     'datif_dz_dev.02_cleaned_uk_argus.argus_print_media_panel',
# ]

# COMMAND ----------

def get_json(df):
    df_result = sqlContext.createDataFrame(df.toJSON(), 'string')
    df_result = df_result.withColumnRenamed('value', 'object')
    df_result = df_result.withColumn('hash_key', F.sha2('object', 256))
    return df_result.select('hash_key', 'object')

# COMMAND ----------

schema = StructType(
    [
        StructField("hash_key", StringType(), False),
        StructField("object", StringType(), False),
        StructField("source_table", StringType(), False),
    ]
)

df_index = sqlContext.createDataFrame(sc.emptyRDD(), schema)

# COMMAND ----------

for t in relevant_tables:
    df = spark.read.table(t)
    df = get_json(df)
    df = df.withColumn('source_table', F.lit(t))
    df_index = df_index.union(df)

# COMMAND ----------

df_index.display()

# COMMAND ----------

df_index.withColumn('len', F.length('object')).orderBy('len', ascending=False).display()

# COMMAND ----------

df_index.count()

# COMMAND ----------

df_index.groupBy('hash_key').count().where('count > 1').count()

# COMMAND ----------

# df_index.write.option("truncate", "true").jdbc(url=jdbcUrl, table=f"{target_schema}.{target_table}", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

db_user = dbutils.secrets.get(scope=secretScope,key="sql-server-admin-name")
db_password = dbutils.secrets.get(scope=secretScope,key="sql-server-admin-password")
jdbc_hostname = dbutils.secrets.get(scope=secretScope, key="sql-server-fqdn")
jdbc_database = dbutils.secrets.get(scope=secretScope, key="sql-db-name") 
jdbc_url = "jdbc:sqlserver://{0}:1433;database={1};".format(jdbc_hostname, jdbc_database)
connectionProperties = {
    "user" : db_user,
    "password" : db_password,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

target_schema = "ai"
target_table = "consolidated_per_row_json_v2"

# COMMAND ----------



# COMMAND ----------

(
    df_index.write
        .option("truncate", "true")
        .jdbc(url=jdbc_url, table=f"{target_schema}.{target_table}", mode="overwrite", properties=connectionProperties)
)

# COMMAND ----------

dbutils.notebook.exit("Finished successfully")
