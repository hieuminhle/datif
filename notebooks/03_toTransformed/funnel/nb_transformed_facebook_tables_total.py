# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Total Aggregation
# MAGIC
# MAGIC Die täglich von der Funnel API abgezogenen Facebook Daten wurden in der DZ unter dem Schema 02_cleaned_uk_facebook für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Total Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggert wird das notebook über die pipeline '03-1100-Funnel_SoMe_daily_parallel'.
# MAGIC
# MAGIC ---
# MAGIC QUELLEN:  
# MAGIC - datif_dz_[dev/tst/prod].02_cleaned_uk_facebook.001_fb_organic_posts_insights_current_view
# MAGIC
# MAGIC ZIEL:  
# MAGIC - 03_transformed.facebook_organic_total
# MAGIC
# MAGIC ---
# MAGIC Versionen (aktuelle immer oben):
# MAGIC - 28.03.2025 Max Mustermann: Init
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

df_cleaned = spark.read.table(f"datif_dz_{env}.02_cleaned_uk_facebook.001_fb_organic_posts_insights_current_view")
df_cleaned = df_cleaned.select(
    F.col("Post_ID__Facebook_Pages").alias("PostID"),
    F.col("Created_Time__Facebook_Pages").alias("CreatedDate"),
    F.col("Post_Message__Facebook_Pages").alias("PostMessage"),
    F.col("Post_Type__Facebook_Pages").alias("PostType"),
    F.col("Post_Permalink_URL__Facebook_Pages").alias("PostURL"),
    F.col("Post_URL__Facebook_Pages").alias("ContentURL"),
    F.col("Post_Total_Impressions_Lifetime__Facebook_Pages").alias("PostTotalImpressionsLifetime").cast('integer'),
    F.col("Post_Likes__Facebook_Pages").alias("TotalPostLikes").cast('integer'),
    F.col("Total_Like_Reactions_of_a_post__Lifetime__Facebook_Pages").alias("TotalLikeReactions").cast('integer'),
    F.col("Total_anger_Reactions_of_a_post__Lifetime__Facebook_Pages").alias("TotalAngerReactions").cast('integer'),
    F.col("Total_haha_Reactions_of_a_post__Lifetime__Facebook_Pages").alias("TotalhahaReactions").cast('integer'),
    F.col("Total_Love_Reactions_of_a_post__Lifetime__Facebook_Pages").alias("TotalLoveReactions").cast('integer'),
    F.col("Total_sad_Reactions_of_a_post__Lifetime__Facebook_Pages").alias("TotalSadReactions").cast('integer'),
    F.col("Total_wow_Reactions_of_a_post__Lifetime__Facebook_Pages").alias("TotalwowReactions").cast('integer'),
    F.col("Post_Shares__Facebook_Pages").alias("TotalPostShares").cast('integer'),
    F.col("Post_Comments__Facebook_Pages").alias("TotalPostComments").cast('integer'),
    F.col("Organic_Video_Views_Lifetime__Facebook_Pages").alias("OrganicVideoViewsLifetime").cast('integer'),
    F.col("Page_Post_Link_Clicks_Lifetime__Facebook_Pages").alias("LinkClicksLifetime").cast('integer'),
    F.col("Unique_Post_Comments___Lifetime_Post__Facebook_Pages").alias("UniquePostCommentsLifetime").cast('integer'),
)

df_cleaned = df_cleaned.withColumn("CreatedDate", date_format(F.col("CreatedDate"), "yyyy-MM-dd").cast("date"))

df_cleaned = df_cleaned.withColumn("CLevelErwaehnungen", c_level_udf(df_cleaned.PostMessage))

df_cleaned = df_cleaned.withColumn(
    "TotalReactions",
    col("TotalLikeReactions") + col("TotalAngerReactions") + col("TotalhahaReactions") + col("TotalLoveReactions") + col("TotalSadReactions") + col("TotalwowReactions")
)
df_cleaned = df_cleaned.withColumn(
    "TotalEngagements",
    (col("TotalPostShares") + col("TotalPostComments") + col("LinkClicksLifetime") + col("TotalReactions"))
)

df_cleaned = df_cleaned.withColumn(
    "TotalWeightedEngagements",
    round(
        (col("TotalPostShares")*0.1
         + col("TotalPostComments")*0.4
         + col("LinkClicksLifetime")*0.3
         + col("TotalReactions")*0.2), 2
    )
)       

df_cleaned = df_cleaned.withColumn(
    "EngagementRateInPercent",
    round(when(col("PostTotalImpressionsLifetime") == 0, 0).otherwise(
        ((col("TotalPostShares")*0.1 + col("TotalPostComments")*0.4 + col("LinkClicksLifetime")*0.3 + col("TotalReactions")*0.2) / col("PostTotalImpressionsLifetime"))*100
    ),2)
)

try:
    print('trying to join already computed tags from the transformed layer')
    df_transformed = spark.read.table("03_transformed.facebook_organic_total").alias("transformed")
    df = join_pre_computed(df_cleaned, df_transformed, "PostID", "Strategie2030", ["tags"])
    print('successfully joined already computed tags from the transformed layer')
except Exception as e:
    if "cannot be found" in str(e) or "cannot be resolved" in str(e):
        print("no pre-computed tags found")
        df = select_tags_or_none(df_cleaned)
    else:
        raise

df = generate_limited(df, "PostID", "CreatedDate", limit_n, "PostMessage", ["tags"])
fn_overwrite_table(df_source=df, target_schema_name=target_schema_name, target_table_name="facebook_organic_total", target_path=target_path)
