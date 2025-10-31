# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel: LinkedIn
# MAGIC
# MAGIC Mit diesem Notebook werden die LinkedIn-Daten (aus der Funnel-API) für _Posts_ und _Videos_ in einer gemeinsamen Tabelle konsolidiert.
# MAGIC
# MAGIC Das Notebook wird täglich von der Pipeline `DaTIF-UK/adf/pipeline/00-1000-Funnel-orchestrate.json` ausgelöst (welche wiederum die Pipeline `03-1100-Funnel_SoMe_daily_parallel` aufruft).
# MAGIC
# MAGIC ---
# MAGIC ## QUELLE  
# MAGIC - Unity-Catalog:
# MAGIC     - `datif_dz_dev.02_cleaned_uk_linkedin.004_li_organic_post_current_view`
# MAGIC     - `datif_dz_dev.02_cleaned_uk_linkedin.005_li_organic_video_current_view`
# MAGIC
# MAGIC ## ZIEL  
# MAGIC - Unity-Catalog:
# MAGIC     - [`datif_pz_uk_dev.03_transformed.linkedin_organic_total`](https://adb-3097416414063338.18.azuredatabricks.net/explore/data/datif_pz_uk_dev/03_transformed/linkedin_organic_total?o=3097416414063338&activeTab=overview)
# MAGIC
# MAGIC ---
# MAGIC ### Versionen (aktuelle immer oben)
# MAGIC - 07.10.2025 Minh Hieu Le: Sicherstellen, keine negativen Zahlen vorkommen
# MAGIC - 13.05.2025 Max Mustermann: Verbesserte Konsolidierte LinkedIn-Daten aus Posts & Videos
# MAGIC - 22.04.2025 Max Mustermann: Init (Clone von [`nb_transformed_funnel_tables_total`](https://adb-3097416414063338.18.azuredatabricks.net/editor/notebooks/2562528655705054?o=3097416414063338#command/2562528655705187))
# MAGIC - 28.03.2025 Max Mustermann: Init

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %run ../../common/nb_tagging_functions

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType
from pyspark.sql.functions import col, when, date_format, to_timestamp, concat, lit, regexp_replace, round
import pyspark.sql.functions as F
from typing import Literal

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utils & Help - parameters & functions

# COMMAND ----------

# Define schema name, table name & path to store tables
target_schema_name = "03_transformed"
target_table_name = "linkedin_organic_total"  # new table, containing consolidated posts & videos (temp. name to avoid overwriting)
target_path = "funnel"
dbutils.widgets.text("p_limit_n", "10")

limit_n = int(dbutils.widgets.get("p_limit_n"))

# COMMAND ----------

# wrapper for tagging
def add_tags(
    df_cleaned: DataFrame, limit_n: int,
    transformed_src: Literal["03_transformed.linkedin_organic_total", "03_transformed.linkedin_organic_video_total"]
) -> DataFrame:
    """Wraps logic for adding strategic topic-tags to the dataframe"""
    check_col = "Strategie2030"
    generate_cols = ["tags"]
    order_col = "CreatedDate"
    id_col = "PostID"
    text_col = "PostContent"
    # ensure that valid transformed_src is passed
    if transformed_src not in ["03_transformed.linkedin_organic_total", "03_transformed.linkedin_organic_video_total"]:
        raise ValueError("Invalid transformed_src")
    
    df_transformed = spark.read.table(transformed_src).alias("transformed")

    try:  # check if pre-computed tags are available
        print('Trying to join pre-computed tags from the transformed layer')
        df_transformed = spark.read.table(transformed_src).alias("transformed")
        df = join_pre_computed(df_cleaned, df_transformed, id_col, check_col, generate_cols)
        print('Successfully joined already computed tags from the transformed layer')
    except Exception as e:  # fill with None & cast to tag-column's type
        if "cannot be found" in str(e) or "cannot be resolved" in str(e):
            print("No pre-computed tags found")
            df = select_tags_or_none(df_cleaned)
        else:
            raise e

    return generate_limited(df, id_col=id_col, order_col=order_col, limit_n=limit_n, text_col=text_col, generate_cols=generate_cols)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ### LinkedIn Organic Consolidated

# COMMAND ----------

shared_columns = [
    'PostID', 'CreatedDate', 'PostContent', 'PostURL', 'ContentType',
    'TotalImpressions', 'TotalLikes', 'TotalShares', 'TotalComments', 'TotalClicks',
    'CLevelErwaehnungen', 'EngagementRateInPercent', 'TotalEngagements', 'TotalWeightedEngagements',	
]

video_only_columns = [
    "TotalViews", "TotalViewers", "TotalTimeWatchedForVideoViews", "TotalTimeWatched"
]
final_columns = shared_columns + video_only_columns

# --- Posts laden ---
df_post = (
    spark.read.table(f"datif_dz_{env}.02_cleaned_uk_linkedin.004_li_organic_post_current_view")
    .groupBy("Post_URN_ID__LinkedIn_Organic")
    .agg(
        F.first("Posted_Date__LinkedIn_Organic").alias("CreatedDate").cast('date'),
        F.first("Post_URN_ID__LinkedIn_Organic").alias("PostID"),
        F.first("Post_Content__LinkedIn_Organic").alias("PostContent"),
        F.first("Post_link__LinkedIn_Organic").alias("PostURL"),
        F.first("Content_Type__LinkedIn_Organic").alias("ContentType"),
        F.sum("Impressions__LinkedIn_Organic").alias("TotalImpressions").cast('integer'),
        F.sum("Likes__LinkedIn_Organic").alias("TotalLikes").cast('integer'),
        F.sum("Shares__LinkedIn_Organic").alias("TotalShares").cast('integer'),
        F.sum("Comments__LinkedIn_Organic").alias("TotalComments").cast('integer'),
        F.sum("Clicks__LinkedIn_Organic").alias("TotalClicks").cast('integer')
    )
    .withColumn("CLevelErwaehnungen", c_level_udf(col("PostContent")))
    .withColumn(
        "ContentType",
        when(col("ContentType") == "Video", "Image").otherwise(col("ContentType"))
    )
    .withColumn("TotalLikes", when(col("TotalLikes") < 0, None).otherwise(col("TotalLikes")))
    .withColumn("TotalShares", when(col("TotalShares") < 0, None).otherwise(col("TotalShares")))
    .withColumn("TotalComments", when(col("TotalComments") < 0, None).otherwise(col("TotalComments")))
    .withColumn(
        "EngagementRateInPercent",
        round(
            when(col("TotalImpressions") == 0, 0).otherwise(
                (col("TotalLikes")*0.1 + col("TotalShares")*0.3 + col("TotalComments")*0.4 + col("TotalClicks")*0.2)
                / col("TotalImpressions")*100
            ),
            2
        )
    )
    .withColumn(
        "TotalEngagements",
        round(
            col("TotalLikes") + col("TotalShares") + col("TotalComments") + col("TotalClicks"),
            2
        )
    )
    .withColumn(
        "TotalWeightedEngagements",
        round(
            (col("TotalLikes")*0.1 + col("TotalShares")*0.3 + col("TotalComments")*0.4 + col("TotalClicks")*0.2),
            2
        )
    )
)

for col_name in video_only_columns:
    df_post = df_post.withColumn(col_name, lit(None).cast("double"))

# --- Videos laden ---
df_video = (
    spark.read.table(f"datif_dz_{env}.02_cleaned_uk_linkedin.005_li_organic_video_current_view")
    .groupBy("Video_ID__LinkedIn_Organic")
    .agg(
        F.first("Video_ID__LinkedIn_Organic").alias("PostID"),
        F.first("Created_Time__LinkedIn_Organic").alias("CreatedDate").cast('date'),
        F.first("Video_Text__LinkedIn_Organic").alias("PostContent"),
        F.first("Video_Post_URL__LinkedIn_Organic").alias("PostURL"),
        F.first("Content_Type__LinkedIn_Organic").alias("ContentType"),
        F.sum("Impressions_Video__LinkedIn_Organic").alias("TotalImpressions").cast('integer'),
        F.sum("Likes_Video__LinkedIn_Organic").alias("TotalLikes").cast('integer'),
        F.sum("Shares_Video__LinkedIn_Organic").alias("TotalShares").cast('integer'),
        F.sum("Comments_Video__LinkedIn_Organic").alias("TotalComments").cast('integer'),
        F.sum("Clicks_Video__LinkedIn_Organic").alias("TotalClicks").cast('integer'),
        F.sum("Views_Video__LinkedIn_Organic").alias("TotalViews").cast('integer'),
        F.sum("Viewers__LinkedIn_Organic").alias("TotalViewers").cast('integer'),
        F.sum("Time_watched_for_video_views__LinkedIn_Organic").alias("TotalTimeWatchedForVideoViews").cast('double'),
        F.sum("Time_Watched__LinkedIn_Organic").alias("TotalTimeWatched").cast('double')
    )
    .withColumn("CLevelErwaehnungen", c_level_udf(col("PostContent")))
    .withColumn("TotalLikes", when(col("TotalLikes") < 0, None).otherwise(col("TotalLikes")))
    .withColumn("TotalShares", when(col("TotalShares") < 0, None).otherwise(col("TotalShares")))
    .withColumn("TotalComments", when(col("TotalComments") < 0, None).otherwise(col("TotalComments")))
    .withColumn(
        "EngagementRateInPercent",
        round(
            when(col("TotalImpressions") == 0, 0).otherwise(
                (col("TotalLikes")*0.1 + col("TotalShares")*0.3 + col("TotalComments")*0.4 + col("TotalClicks")*0.2)
                / col("TotalImpressions")*100
            ),
            2
        )
    )
    .withColumn(
        "TotalEngagements",
        round(
            col("TotalLikes") + col("TotalShares") + col("TotalComments") + col("TotalClicks"),
            2
        )
    )
    .withColumn(
        "TotalWeightedEngagements",
        round(
            (col("TotalLikes")*0.1 + col("TotalShares")*0.3 + col("TotalComments")*0.4 + col("TotalClicks")*0.2),
            2
        )
    )
)

# --- Kombinieren ---
df_combined = (
    df_post.select(final_columns)
    .unionByName(df_video.select(final_columns))
    .withColumn("Source", when(col("TotalViews").isNotNull(), "Video").otherwise("Post"))
)

try:
    print('trying to join already computed tags from the transformed layer')
    df_transformed = spark.read.table("03_transformed.linkedin_organic_total").alias("transformed")
    df = join_pre_computed(df_combined, df_transformed, "PostID", "Strategie2030", ["tags"])
    print('successfully joined already computed tags from the transformed layer')
except Exception as e:
    if "cannot be found" in str(e) or "cannot be resolved" in str(e):
        print("no pre-computed tags found")
        df = select_tags_or_none(df_combined)
    else:
        raise e

df_combined = generate_limited(df, "PostID", "CreatedDate", limit_n, "PostContent", ["tags"])

# --- Speichern ---
fn_overwrite_table(
    df_source=df_combined,
    target_schema_name=target_schema_name,
    target_table_name="linkedin_organic_total",
    target_path=target_path
)
