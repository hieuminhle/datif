# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Total Aggregation
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der Funnel API abgezogenen Daten der Social Media Daten wurden in der DZ unter dem Schema 02_cleaned für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Total Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggerde wird das notebook über die pipeline '00-1000-funnel-orchestrate'.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_dz_dev.02_cleaned_uk_facebook.001_fb_organic_posts_insights_current_view
# MAGIC - datif_dz_dev.02_cleaned_uk_instagram.002_ig_organic_post_current_view
# MAGIC - datif_dz_dev.02_cleaned_uk_instagram.ig_stories_adverity_current_view
# MAGIC - datif_dz_dev.02_cleaned_uk_x.003_x_organic_post_current_view
# MAGIC - datif_dz_dev.02_cleaned_uk_linkedin.004_li_organic_post_current_view
# MAGIC - datif_dz_dev.02_cleaned_uk_linkedin.005_li_organic_video_current_view
# MAGIC - datif_dz_dev.02_cleaned_uk_yt.007_yt_organic_post_current_view
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_dev.03_transformed.facebook_organic_total
# MAGIC - datif_pz_uk_dev.03_transformed.instagram_organic_total
# MAGIC - datif_pz_uk_dev.03_transformed.instagram_organic_stories_total
# MAGIC - datif_pz_uk_dev.03_transformed.x_organic_total
# MAGIC - datif_pz_uk_dev.03_transformed.linkedin_organic_total
# MAGIC - datif_pz_uk_dev.03_transformed.youtube_organic_post_total
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC - Versionen (aktuelle immer oben):
# MAGIC - 27.05.2025 Justin Stange-Heiduk: Youtube Themenbereiche hinzugefügt, Performancekultur entfernt
# MAGIC - 13.05.2025 Justin Stange-Heiduk: Verbesserte Konsolidierte LinkedIn-Daten aus Posts & Videos
# MAGIC - 07.05.2025 Justin Stange-Heiduk: Instagram API Änderung Impression -> Views
# MAGIC - 23.04.2025 Philipp Sandhaas: Konsolidierte LinkedIn-Daten aus Posts & Videos
# MAGIC - 28.02.2025 Sebastian Fastert: Schema Updates YouTube
# MAGIC - 13.02.2025 Svenja Schuder: Schema Updates
# MAGIC - 06.02.2025 Svenja Schuder: Unity Catalog als Variable hinzugefügt
# MAGIC - 06.12.2024 Svenja Schuder: C-Level-Tagging hinzugefügt
# MAGIC - 03.12.2024 Svenja Schuder: Schemabereinigungen
# MAGIC - 29.11.2024 Svenja Schuder: Refactoring
# MAGIC - 13.11.2024 Sebastian Fastert: Logik Strategische Themen hinzugefügt
# MAGIC - 03.11.2024 Svenja Schuder: Init
# MAGIC

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
limit_n = 10

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facebook Organic Posts

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instagram Organic Post

# COMMAND ----------

df_cleaned = spark.read.table(f"datif_dz_{env}.02_cleaned_uk_instagram.002_ig_organic_post_current_view")
# join der Spalten Impressions und Plays
#df_cleaned = df_cleaned.withColumn("Views__Instagram_Insights", when(F.col("Views__Instagram_Insights") == 0, F.col("Plays__Instagram_Insights")).otherwise(F.col("Views__Instagram_Insights"))) 
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
df_ig_stories_cleaned = df_ig_stories_cleaned.select(
    F.col("Date").cast("date").alias("CreatedDate"),
    F.col("Story_ID__Instagram_Stories").alias("StoryID"),
    F.col("Media_Type__Instagram_Stories").alias("MediaType"),
    F.col("Username__Instagram_Stories").alias("Username"),
    F.when(F.col("Caption__Instagram_Stories") == "", None).otherwise(F.col("Caption__Instagram_Stories")).alias("Caption"),
    F.col("Impressions__Instagram_Stories").cast("int").alias("TotalImpressions"),
    #F.col("Views__Instagram_Stories").cast("int").alias("TotalViews"),
    F.col("Replies__Instagram_Stories").cast("int").alias("TotalReplies"),
    F.col("Shares__Instagram_Stories").cast("int").alias("TotalShares"),
    F.col("Swipe_Forward__Instagram_Stories").cast("int").alias("TotalSwipesForward"),
    F.col("Tap_Back__Instagram_Stories").cast("int").alias("TotalTapBack"),
    F.col("Tap_Exit__Instagram_Stories").cast("int").alias("TotalTabExit"),
    F.col("Tap_Forward__Instagram_Stories").cast("int").alias("TotalTapForward"),
    F.col("Reach__Instagram_Stories").cast("int").alias("TotalReach")
)

df_ig_stories_cleaned = df_ig_stories_cleaned.withColumn(
    "TemporaryEngagementRateInPercent", 
    round(when(col("TotalImpressions") == 0, 0).otherwise(
        (col("TotalReplies")*0.5 + col("TotalShares")*0.2)/col("TotalImpressions")*100
    ),2)
)

fn_overwrite_table(df_source=df_ig_stories_cleaned, target_schema_name=target_schema_name, target_table_name="instagram_organic_stories_total", target_path=target_path)

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
    F.sum("Engagements__X_Organic").alias("TotalEngagements").cast('integer'),
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
    "EngagementRateInPercent",
    round(when(col("TotalImpressions") == 0, 0).otherwise(
        (col("TotalInteractions")*0.35 + col("TotalReplies")*0.45 + col("TotalReposts")*0.2)/col("TotalImpressions")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## LinkedIn Organic (consolidated)

# COMMAND ----------

from pyspark.sql import DataFrame
from typing import Literal

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
    if transformed_src not in ["03_transformed.linkedin_organic_total", "03_transformed.linkedin_organic_video_total", "03_transformed.linkedin_organic_post_total"]:
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
# MAGIC ### LinkedIn Consolidated

# COMMAND ----------

shared_columns = [
    'PostID', 'CreatedDate', 'PostContent', 'PostURL', 'ContentType',
    'TotalImpressions', 'TotalLikes', 'TotalShares', 'TotalComments', 'TotalClicks',
    'CLevelErwaehnungen', 'EngagementRateInPercent'
]

video_only_columns = ["TotalViews", "TotalViewers", "TotalTimeWatchedForVideoViews", "TotalTimeWatched"]
final_columns = shared_columns + video_only_columns

# --- Posts laden ---
df_post = spark.read.table(f"datif_dz_{env}.02_cleaned_uk_linkedin.004_li_organic_post_current_view") \
    .groupBy("Post_URN_ID__LinkedIn_Organic").agg(
        F.first("Posted_Date__LinkedIn_Organic").alias("CreatedDate").cast('date'),
        F.first("Post_URN_ID__LinkedIn_Organic").alias("PostID"),
        F.first("Post_Content__LinkedIn_Organic").alias("PostContent"),
        F.first("Post_link__LinkedIn_Organic").alias("PostURL"),
        F.first("Content_Type__LinkedIn_Organic").alias("ContentType"),
        F.coalesce(F.sum("Impressions__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalImpressions"),
        F.coalesce(F.sum("Likes__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalLikes"),
        F.coalesce(F.sum("Shares__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalShares"),
        F.coalesce(F.sum("Comments__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalComments"),
        F.coalesce(F.sum("Clicks__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalClicks")
    ) \
    .withColumn("CLevelErwaehnungen", c_level_udf(col("PostContent"))) \
    .withColumn("ContentType", when(col("ContentType") == "Video", "Image").otherwise(col("ContentType"))) \
    .withColumn("EngagementRateInPercent", round(
        when(col("TotalImpressions") == 0, 0).otherwise(
            (col("TotalLikes")*0.1 + col("TotalShares")*0.3 + col("TotalComments")*0.4 + col("TotalClicks")*0.2)
            / col("TotalImpressions")*100
        ), 2))

for col_name in video_only_columns:
    df_post = df_post.withColumn(col_name, lit(None).cast("double"))

# --- Videos laden ---
df_video = spark.read.table(f"datif_dz_{env}.02_cleaned_uk_linkedin.005_li_organic_video_current_view") \
    .groupBy("Video_ID__LinkedIn_Organic").agg(
        F.first("Video_ID__LinkedIn_Organic").alias("PostID"),
        F.first("Created_Time__LinkedIn_Organic").alias("CreatedDate").cast('date'),
        F.first("Video_Text__LinkedIn_Organic").alias("PostContent"),
        F.first("Video_Post_URL__LinkedIn_Organic").alias("PostURL"),
        F.first("Content_Type__LinkedIn_Organic").alias("ContentType"),
        F.coalesce(F.sum("Impressions_Video__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalImpressions"),
        F.coalesce(F.sum("Likes_Video__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalLikes"),
        F.coalesce(F.sum("Shares_Video__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalShares"),
        F.coalesce(F.sum("Comments_Video__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalComments"),
        F.coalesce(F.sum("Clicks_Video__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalClicks"),
        F.coalesce(F.sum("Views_Video__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalViews"),
        F.coalesce(F.sum("Viewers__LinkedIn_Organic"), F.lit(0)).cast("integer").alias("TotalViewers"),
        F.coalesce(F.sum("Time_watched_for_video_views__LinkedIn_Organic"), F.lit(0.0)).cast("double").alias("TotalTimeWatchedForVideoViews"),
        F.coalesce(F.sum("Time_Watched__LinkedIn_Organic"), F.lit(0.0)).cast("double").alias("TotalTimeWatched")
    ) \
    .withColumn("CLevelErwaehnungen", c_level_udf(col("PostContent"))) \
    .withColumn("EngagementRateInPercent", round(
        when(col("TotalImpressions") == 0, 0).otherwise(
            (col("TotalLikes")*0.1 + col("TotalShares")*0.3 + col("TotalComments")*0.4 + col("TotalClicks")*0.2)
            / col("TotalImpressions")*100
        ), 2))

# --- Kombinieren ---
df_combined = df_post.select(final_columns).unionByName(df_video.select(final_columns)) \
    .withColumn("Source", when(col("TotalViews").isNotNull(), "Video").otherwise("Post"))



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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Youtube Organic Post

# COMMAND ----------

df_cleaned = spark.read.table(f"datif_dz_{env}.02_cleaned_uk_yt.007_yt_organic_post_current_view")
df_cleaned = df_cleaned.groupBy("Video_ID__YouTube").agg(
    F.first("Published_At__YouTube").cast("date").alias("CreatedDate"),
    F.first("Video_ID__YouTube").alias("VideoID"),
    F.first("Channel_ID__YouTube").alias("ChannelID"),
    F.concat(F.lit("https://www.youtube.com/watch?v="), F.first("Video_ID__YouTube")).alias("VideoURL"),
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

df_cleaned = df_cleaned.withColumn("TotalAverageViewDuration", round((df_cleaned.TotalEstimatedMinutesWatched*60/df_cleaned.TotalViews),2))
df_cleaned = tag_strategic_areas_exact_matches(df_cleaned, "Tags")
df_cleaned = df_cleaned.withColumn("CLevelErwaehnungen", c_level_udf(df_cleaned.VideoDescription))

df_cleaned = df_cleaned.withColumn("AverageMinutesWatched", 
    round(when(col("TotalViews") == 0, 0).otherwise(
        col("TotalEstimatedMinutesWatched")/col("TotalViews")
    ),2)
)

df_cleaned = df_cleaned.withColumn("EngagementRateInPercent", 
    round(when(col("TotalViews") == 0, 0).otherwise(
        round((col("TotalLikes")*0.2 + col("TotalDislikes")*0.05 + col("TotalComments")*0.25 + col("TotalShares")*0.15 + col("AverageMinutesWatched")*0.35)/col("TotalViews")*100, 2)
    ),2)
)

try:
    print('trying to join already computed tags from the transformed layer')
    df_transformed = spark.read.table("03_transformed.youtube_organic_post_total").alias("transformed")
    df = join_pre_computed(df_cleaned, df_transformed, "VideoID", "Themenbereich1", ["tags"])
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
