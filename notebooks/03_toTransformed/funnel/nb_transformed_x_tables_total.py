# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Total Aggregation
# MAGIC
# MAGIC Die täglich von der Funnel API abgezogenen X Daten wurden in der DZ unter dem Schema 02_cleaned_uk_x für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Total Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggert wird das notebook über die pipeline '03-1100-Funnel_SoMe_daily_parallel'.
# MAGIC
# MAGIC ---
# MAGIC QUELLEN:  
# MAGIC
# MAGIC - datif_dz_[dev/tst/prod].02_cleaned_uk_x.003_x_organic_post_current_view
# MAGIC
# MAGIC ZIEL:
# MAGIC - datif_pz_uk_dev.03_transformed.x_organic_total
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC Versionen (aktuelle immer oben):
# MAGIC - 16.10.2025 Minh Hieu Le: Hinzufügen von Engagement und WeightedEngagement
# MAGIC - 28.03.2025 Sebastian Fastert: Init
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_limit_n", "10")

limit_n = int(dbutils.widgets.get("p_limit_n"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %run ../../common/nb_tagging_functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType
from pyspark.sql.functions import col, when, date_format, to_timestamp, concat, lit, regexp_replace, round
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Parameters

# COMMAND ----------

# Define schema name and path to store tables
target_schema_name = "03_transformed"
target_path = "funnel"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## X Organic Post

# COMMAND ----------

df_cleaned = spark.read.table(f"datif_dz_{env}.02_cleaned_uk_x.003_x_organic_post_current_view")
df_cleaned = df_cleaned.groupBy("Post_ID__X_Organic").agg(
    F.first("Post_ID__X_Organic").alias("PostID"),
    F.first("Created_At__X_Organic").alias("CreatedDate"),
    F.first("In_Reply_To_Status_ID__X_Organic").alias("InReplyToStatusID"),
    F.first("Post_Text__X_Organic").alias("PostMessage"),
    F.first("Post_Media_URL__X_Organic").alias("PostURL"),
    F.first("Post_Media_Type__X_Organic").alias("PostType"),
    F.sum("Impressions__X_Organic").alias("TotalImpressions").cast('integer'),
    F.sum("Likes__X_Organic").alias("TotalLikes").cast('integer'),
    F.sum("Reposts__X_Organic").alias("TotalReposts").cast('integer'),
    F.sum("Replies__X_Organic").alias("TotalReplies").cast('integer'),
    F.sum("Follows__X_Organic").alias("TotalFollows").cast('integer'),
    F.sum("Clicks__X_Organic").alias("TotalClicks").cast('integer'),
    F.sum("Link_Clicks__X_Organic").alias("TotalLinkClicks").cast('integer'),
    F.sum("App_Clicks__X_Organic").alias("TotalAppClicks").cast('integer'),
    F.sum("Card_Engagements__X_Organic").alias("TotalCardEngagements").cast('integer')
).drop("Post_ID__X_Organic").alias("cleaned")


df_cleaned = df_cleaned.withColumn("CreatedDate", regexp_replace('CreatedDate', r'^[A-Za-z]{3} ', ''))\
.withColumn("CreatedDate",
    F.when(F.col("CreatedDate").rlike("[A-Za-z]{3} [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \\+[0-9]{4} [0-9]{4}$"), to_timestamp("CreatedDate", "MMM dd HH:mm:ss Z yyyy"))
    .when(F.col("CreatedDate").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z$"), to_timestamp("CreatedDate", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))\
.withColumn("CreatedDate", date_format(F.col("CreatedDate"), "yyyy-MM-dd").cast("date"))\
.withColumn("PostURL", concat(lit("https://x.com/EnBW/status/"), df_cleaned.PostID))

df_cleaned = df_cleaned.withColumn("CLevelErwaehnungen", c_level_udf(df_cleaned.PostMessage))

df_cleaned = df_cleaned.withColumn("TotalInteractions", col("TotalLikes") + col("TotalClicks"))

df_cleaned = df_cleaned.withColumn(
    "TotalEngagements",
    col("TotalInteractions") + col("TotalReplies") + col("TotalReposts")
)

df_cleaned = df_cleaned.withColumn(
    "WeightedEngagements",
    col("TotalInteractions")*0.35 + col("TotalReplies")*0.45 + col("TotalReposts")*0.2
)

df_cleaned = df_cleaned.withColumn(
    "EngagementRateInPercent",
    round(when(col("TotalImpressions") == 0, 0).otherwise(
        (col("TotalInteractions")*0.35 + col("TotalReplies")*0.45 + col("TotalReposts")*0.2)/col("TotalImpressions")*100
    ),2)
)

try:
    print('trying to join already computed tags from the transformed layer')
    df_transformed = spark.read.table("03_transformed.x_organic_total").alias("transformed")
    df = join_pre_computed(df_cleaned, df_transformed, "PostID", "Strategie2030", ["tags"])
    print('successfully joined already computed tags from the transformed layer')
except Exception as e:
    print(e)
    if "cannot be found" in str(e) or "cannot be resolved" in str(e):
        print("no pre-computed tags found")
        df = select_tags_or_none(df_cleaned)
    else:
        raise e

df = generate_limited(df, "PostID", "CreatedDate", limit_n, "PostMessage", ["tags"])
#display(df)
# write df as table
fn_overwrite_table(df_source=df, target_schema_name=target_schema_name, target_table_name="x_organic_total", target_path=target_path)
