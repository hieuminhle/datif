# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Total Aggregation
# MAGIC
# MAGIC Die täglich von der Funnel API abgezogenen YouTube Daten wurden in der DZ unter dem Schema 02_cleaned_uk_yt für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Total Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggert wird das notebook über die pipeline '03-1100-Funnel_SoMe_daily_parallel'.
# MAGIC
# MAGIC ---
# MAGIC QUELLEN:  
# MAGIC - datif_dz_dev.02_cleaned_uk_yt.007_yt_organic_post_current_view
# MAGIC
# MAGIC ZIEL:
# MAGIC - datif_pz_uk_dev.03_transformed.youtube_organic_post_total
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC Versionen (aktuelle immer oben):
# MAGIC - 16.10.2025 Minh Hieu Le: Anpassen, sodass TotalAverageViewDuration nie durch 0 geteilt wird
# MAGIC - 25.06.2025 Max Mustermann: Youtube Owner Spalte hinzugefügt
# MAGIC - 27.05.2025 Max Mustermann: Youtube Themenbereiche hinzugefügt
# MAGIC - 06.05.2025 Minh Hieu Le: VideoURL hinzugefügt
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
from pyspark.sql.functions import round as spark_round

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

df_cleaned = spark.read.table(f"datif_dz_{env}.02_cleaned_uk_yt.007_yt_organic_post_current_view")
df_cleaned = df_cleaned.groupBy("Video_ID__YouTube").agg(
    F.first("Published_At__YouTube").cast("date").alias("CreatedDate"),
    F.first("Video_ID__YouTube").alias("VideoID"),
    F.concat(F.lit("https://www.youtube.com/watch?v="), F.first("Video_ID__YouTube")).alias("VideoURL"),
    F.first("Channel_ID__YouTube").alias("ChannelID"),
    F.first("Video_Title__YouTube").alias("VideoTitle"),
    F.first("Tags__YouTube").alias("Tags"),
    F.first("Video_Description__YouTube").alias("VideoDescription"),
    F.first("Duration_HH_MM_SS__YouTube").alias("Duration"),
    F.sum("Views__YouTube").alias("TotalViews").cast('integer'),
    F.sum("Likes__YouTube").alias("TotalLikes").cast('integer'),
    F.sum("Dislikes__YouTube").alias("TotalDislikes").cast('integer'),
    F.sum("Comments__YouTube").alias("TotalComments").cast('integer'),
    F.sum("Shares__YouTube").alias("TotalShares").cast('integer'),
    F.sum("Subscribers_Gained__YouTube").alias("TotalSubscribersGained").cast('integer'),
    F.sum("Subscribers_Lost__YouTube").alias("TotalSubscribersLost").cast('integer'),
    F.sum("Card_Impressions__YouTube").alias("TotalCardImpressions").cast('integer'),
    F.sum("Card_Teaser_Impressions__YouTube").alias("TotalCardTeaserImpressions").cast('integer'),
    F.sum("Annotation_Clickable_Impressions__YouTube").alias("TotalAnnotationClickableImpressions").cast('integer'),
    F.sum("Annotation_Closable_Impressions__YouTube").alias("TotalAnnotationClosableImpressions").cast('integer'),
    # F.sum("Estimated_Minutes_Watched_YouTube_Premium__YouTube").alias("TotalEstimatedMinutesWatchedPremium").cast('integer'),
    F.sum("Estimated_Minutes_Watched__YouTube").alias("TotalEstimatedMinutesWatched").cast('integer'),
    # F.sum("Videos_Added_to_Playlists__YouTube").alias("TotalVideosAddedToPlaylists").cast('integer'),
    # F.sum("Videos_Removed_from_Playlists__YouTube").alias("TotalVideosRemovedFromPlaylists").cast('integer'),
    F.sum("Views_YouTube_Premium__YouTube").alias("TotalViewsPremium").cast('integer'),
    F.sum("Annotation_Clicks__YouTube").alias("TotalAnnotationClicks").cast('integer')
).drop("Video_ID__YouTube").alias("cleaned")

df_cleaned = df_cleaned.withColumn("TotalAverageViewDuration", round(when(col("TotalViews") == 0, 0).otherwise(col("TotalEstimatedMinutesWatched") * 60 / col("TotalViews")), 2))

df_cleaned = tag_strategic_areas_exact_matches(df_cleaned, "Tags")

display(df_cleaned)

df_cleaned = df_cleaned.withColumn(
    "VideoDescription",
    when(
        (col("VideoDescription").isNull()) | (col("VideoDescription") == ""),
        lit("Keine Videobeschreibung verfügbar")
    ).otherwise(col("VideoDescription"))
)
df_cleaned = df_cleaned.withColumn("CLevelErwaehnungen", c_level_udf(df_cleaned.VideoDescription))

df_cleaned = df_cleaned.withColumn("AverageMinutesWatched", 
    round(when(col("TotalViews") == 0, 0).otherwise(
        col("TotalEstimatedMinutesWatched")/col("TotalViews")
    ),2)
)

df_cleaned = df_cleaned.withColumn("EngagementRateInPercent", 
    round(when(col("TotalViews") == 0, 0).otherwise(
        (col("TotalLikes")*0.2 + col("TotalDislikes")*0.05 + col("TotalComments")*0.25 + col("TotalShares")*0.15 + col("AverageMinutesWatched")*0.35)/col("TotalViews")*100
    ),2)
)

df_cleaned = df_cleaned.withColumn("TotalEngagements", 
    round(when(col("TotalViews") == 0, 0).otherwise(
        (col("TotalLikes") + col("TotalDislikes") + col("TotalComments") + col("TotalShares") + col("AverageMinutesWatched"))
    ),2)
)

df_cleaned = df_cleaned.withColumn("TotalWeightedEngagements", 
    round(when(col("TotalViews") == 0, 0).otherwise(
        (col("TotalLikes")*0.2 + col("TotalDislikes")*0.05 + col("TotalComments")*0.25 + col("TotalShares")*0.15 + col("AverageMinutesWatched")*0.35)
    ),2)
)

try:
    print('trying to join already computed tags from the transformed layer')
    df_transformed = spark.read.table("03_transformed.youtube_organic_post_total").alias("transformed")
    df = join_pre_computed_youtube(df_cleaned, df_transformed, "VideoID", "Themenbereich1", ["tags"])
    print('successfully joined already computed tags from the transformed layer')
except Exception as e:
    if "cannot be found" in str(e) or "cannot be resolved" in str(e):
        print("no pre-computed tags found")
        df = select_tags_or_none(df_cleaned)
    else:
        raise e

df = generate_limited_youtube(df, "VideoID", "CreatedDate", limit_n, "VideoDescription", ["tags"])

df = df.withColumn(
    "recompute_tags",
    F.when(F.col("VideoDescription").isNull(), F.lit(False))
    .otherwise(F.col("recompute_tags"))
)


# write df as table
fn_overwrite_table(df_source=df, target_schema_name=target_schema_name, target_table_name="youtube_organic_post_total", target_path=target_path)
