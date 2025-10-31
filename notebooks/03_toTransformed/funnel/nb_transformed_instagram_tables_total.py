# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Total Aggregation
# MAGIC
# MAGIC Die täglich von der Funnel API abgezogenen Instagram Daten wurden in der DZ unter dem Schema 02_cleaned_uk_instagram für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Total Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggert wird das notebook über die pipeline '03-1100-Funnel_SoMe_daily_parallel'.
# MAGIC
# MAGIC ---
# MAGIC QUELLEN:  
# MAGIC Unity-Catalog:
# MAGIC
# MAGIC - datif_dz_[dev/tst/prod].02_cleaned_uk_instagram.002_ig_organic_post_current_view
# MAGIC - datif_dz_[dev/tst/prod].02_cleaned_uk_instagram.ig_stories_adverity_current_view
# MAGIC
# MAGIC ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_dev.03_transformed.instagram_organic_total
# MAGIC - datif_pz_uk_dev.03_transformed.instagram_organic_stories_total
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 16.10.2025 Minh Hieu Le: Korrektur der Formel von WeightedEngagement und EngagementRate für Insta Stories
# MAGIC - 07.05.2025 Justin Stange-Heiduk: Instagram API Änderung Impression -> Views
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
# MAGIC ## Instagram Organic Post

# COMMAND ----------

df_cleaned = spark.read.table(f"datif_dz_{env}.02_cleaned_uk_instagram.002_ig_organic_post_current_view")
# join der Spalten Impressions und Plays
df_cleaned = df_cleaned.withColumn(
    "Views__Instagram_Insights",
    when((F.col("Views__Instagram_Insights").isNull()) | (F.col("Views__Instagram_Insights") == 0),
         when((F.col("Plays__Instagram_Insights").isNull()) | (F.col("Plays__Instagram_Insights") == 0),
              F.col("Impressions__Instagram_Insights")
         ).otherwise(F.col("Plays__Instagram_Insights"))
    ).otherwise(F.col("Views__Instagram_Insights"))
)
df_cleaned = df_cleaned.select(
    F.col("Media_ID__Instagram_Insights").alias("PostID"),
    F.col("Date").alias("CreatedDate").cast('date'),
    F.col("Caption__Instagram_Insights").alias("PostMessage"),
    F.col("Permalink__Instagram_Insights").alias("PostURL"),
    F.col("Media_type__Instagram_Insights").alias("PostType"),
    F.col("Views__Instagram_Insights").alias("TotalViews").cast('integer'),
    F.col("Likes__Instagram_Insights").alias("TotalLikes").cast('integer'),
    F.col("Shares__Instagram_Insights").alias("TotalShares").cast('integer'),
    F.col("Comments__Instagram_Insights").alias("TotalComments").cast('integer'),
    F.col("Saved__Instagram_Insights").alias("TotalSaved").cast('integer'),
    F.col("Total_Interactions__Instagram_Insights").alias("TotalInteractions").cast('integer')
)

df_cleaned = df_cleaned.withColumn("CLevelErwaehnungen", c_level_udf(df_cleaned.PostMessage))

df_cleaned = df_cleaned.withColumn(
    "TotalEngagements", 
    round(
        ((col("TotalLikes") + col("TotalComments") + col("TotalShares") + col("TotalSaved"))
    ),2)
)

df_cleaned = df_cleaned.withColumn(
    "TotalWeightedEngagements", 
    round(
        ((col("TotalLikes")*0.2 + col("TotalComments")*0.4 + col("TotalShares")*0.1 + col("TotalSaved")*0.3)
    ),2)
)

df_cleaned = df_cleaned.withColumn(
    "EngagementRateInPercent", 
    round(when(col("TotalViews") == 0, 0).otherwise(
        ((col("TotalLikes")*0.2 + col("TotalComments")*0.4 + col("TotalShares")*0.1 + col("TotalSaved")*0.3)/col("TotalViews"))*100
    ),2)
)

try:
    print('trying to join already computed tags from the transformed layer')
    df_transformed = spark.read.table("03_transformed.instagram_organic_total").alias("transformed")
    df = join_pre_computed(df_cleaned, df_transformed, "PostID", "Strategie2030", ["tags"])
    print('successfully joined already computed tags from the transformed layer')
except Exception as e:
    if "cannot be found" in str(e) or "cannot be resolved" in str(e):
        print("no pre-computed tags found")
        df = select_tags_or_none(df_cleaned)
    else:
        raise e

df = generate_limited(df, "PostID", "CreatedDate", limit_n, "PostMessage", ["tags"])
#df.display()
fn_overwrite_table(df_source=df, target_schema_name=target_schema_name, target_table_name="instagram_organic_total", target_path=target_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instagram Stories

# COMMAND ----------

df_ig_stories_cleaned = spark.read.table(f"datif_dz_{env}.02_cleaned_uk_instagram.011_ig_organic_stories_current_view")
print(df_ig_stories_cleaned.columns)
df_ig_stories_cleaned = df_ig_stories_cleaned.withColumn(
    "Views__Instagram_Stories",
    F.when(
        (F.col("Impressions__Instagram_Stories").isNull()) | (F.col("Impressions__Instagram_Stories") == 0),
        F.when(
            (F.col("Views__Instagram_Stories").isNull()) | (F.col("Views__Instagram_Stories") == 0),
            F.col("Reach__Instagram_Stories")
        ).otherwise(F.col("Views__Instagram_Stories"))
    ).otherwise(F.col("Impressions__Instagram_Stories"))
)


df_ig_stories_cleaned = df_ig_stories_cleaned.select(
    F.col("Date").cast("date").alias("CreatedDate"),
    F.col("Story_ID__Instagram_Stories").alias("StoryID"),
    F.col("Media_Type__Instagram_Stories").alias("MediaType"),
    F.col("Username__Instagram_Stories").alias("Username"),
    F.col("Caption__Instagram_Stories").alias("Caption"),
    F.col("Views__Instagram_Stories").cast("int").alias("TotalImpressions"),
    # F.col("Impressions__Instagram_Stories").cast("int").alias("TotalImpressions"),
    # F.col("Views__Instagram_Stories").cast("int").alias("TotalViews"),
    F.col("Replies__Instagram_Stories").cast("int").alias("TotalReplies"),
    F.col("Shares__Instagram_Stories").cast("int").alias("TotalShares"),
    F.col("Swipe_Forward__Instagram_Stories").cast("int").alias("TotalSwipesForward"),
    F.col("Tap_Back__Instagram_Stories").cast("int").alias("TotalTapBack"),
    F.col("Tap_Exit__Instagram_Stories").cast("int").alias("TotalTabExit"),
    F.col("Tap_Forward__Instagram_Stories").cast("int").alias("TotalTapForward"),
    F.col("Reach__Instagram_Stories").cast("int").alias("TotalReach")
)
df_ig_stories_cleaned = df_ig_stories_cleaned.withColumn(
    "TotalEngagements", 
    round(
        (col("TotalReplies") + col("TotalShares"))
    ,2)
)

df_ig_stories_cleaned = df_ig_stories_cleaned.withColumn(
    "TotalWeightedEngagements", 
    round(
        (col("TotalReplies")*0.8 + col("TotalShares")*0.2)
    ,2)
)

df_ig_stories_cleaned = df_ig_stories_cleaned.withColumn(
    "TemporaryEngagementRateInPercent", 
    round(when(col("TotalImpressions") == 0, 0).otherwise(
        (col("TotalReplies")*0.8 + col("TotalShares")*0.2)/col("TotalImpressions")*100
    ),2)
)

fn_overwrite_table(df_source=df_ig_stories_cleaned, target_schema_name=target_schema_name, target_table_name="instagram_organic_stories_total", target_path=target_path)

# COMMAND ----------

display(df_ig_stories_cleaned)
