# Databricks notebook source
# MAGIC %run ../common/nb_init

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.types as T
import pyspark.sql.functions as F

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

#target_schema = "ai"
#target_table = "consolidated_socials"

# Setze den korrekten Zielpfad für Unity Catalog
target_table_path = "dbfs:/mnt/datif_pz_uk_dev/03_served/consolidated_socials"

# COMMAND ----------

relevant_tables = [ # auf die current view schauen
    'social.enriched_facebook_organic_contents',
    'social.enriched_instagram_organic_contents',
    'social.enriched_twitter_organic_contents',
    'social.enriched_linkedin_organic_contents',
]


# COMMAND ----------

df_socials = spark.read.jdbc(url=jdbc_url, table=relevant_tables[0], properties=connectionProperties)
for t in relevant_tables[1:]:
    df = spark.read.jdbc(url=jdbc_url, table=t, properties=connectionProperties)
    df = df.drop('benchmark_social_posts')
    df = df.drop('unique_impressions')
    df_socials = df_socials.unionByName(df)

if 'engagements' in df_socials.columns:
    df_socials = df_socials.withColumn("engagements", df_socials["engagements"].cast("float"))
    df_socials = df_socials.withColumn("benchmark_comments", df_socials["benchmark_comments"].cast("float"))

# COMMAND ----------

# Überprüfe das Schema, um sicherzustellen, dass die Änderung übernommen wurde
df_socials.printSchema()

# COMMAND ----------

'''
(
    df_socials.write
        .option("truncate", "true")
        .jdbc(url=jdbc_url, table=f"{target_schema}.{target_table}", mode="overwrite", properties=connectionProperties)
)
'''

# Schreiben in eine Delta-Tabelle in Unity Catalog
df_socials.write.saveAsTable("datif_pz_uk_dev.03_served.consolidated_socials", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("Finished successfully")
