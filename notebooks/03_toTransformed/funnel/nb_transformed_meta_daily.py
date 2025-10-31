# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Daily
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der Funnel API abgezogenen Daten der Social Media Daten wurden in der DZ unter dem Schema 02_cleaned für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die daily Meta (Instagram und Facebook) Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggerde wird das notebook über die pipeline '00-1000-Funnel-orchestrate'.
# MAGIC
# MAGIC ---
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 03_transformed.facebook_organic_scd2_view
# MAGIC - 03_transformed.instagram_organic_scd2_view
# MAGIC - 03_transformed.instagram_organic_stories_scd2_view
# MAGIC
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 03_transformed.facebook_organic_daily
# MAGIC - 03_transformed.instagram_organic_post_daily
# MAGIC - 03_transformed.instagram_organic_stories_daily
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 21.10.2025 Justin Stange-Heiduk: Add Daily & ID agg
# MAGIC - 15.10.2025 Minh Hieu Le: Correct the formula for calculating engagement for Instagram Posts
# MAGIC - 09.10.2025 Minh Hieu Le: Correct the formula for calculating engagement for Facebook
# MAGIC - 07.10.2025 Minh Hieu Le: Display columns which are used for calculating engagement
# MAGIC - 11.06.2025 Justin Stange-Heiduk: Add EngagementRating und WeightedEngagamenet to not Meta Channel
# MAGIC - 10.06.2025 Justin Stange-Heiduk: Init
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Parameters

# COMMAND ----------

pz_target_schema_name = "03_transformed"
target_path = "funnel"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facebook Organic Posts

# COMMAND ----------

# load data from PZ-03-transformed into dataframe
df_facebook_daily = spark.sql(f"""
                              SELECT
                                    Date,
                                    PostID,
                                    CreatedDate,                                    
                                    PostMessage,
                                    PostURL,
                                    PostType,
                                    PostTotalImpressionsLifetime,
                                    TotalPostLikes,
                                    TotalLikeReactions,
                                    TotalAngerReactions,
                                    TotalhahaReactions,
                                    TotalLoveReactions,
                                    TotalSadReactions,
                                    TotalwowReactions,
                                    TotalPostShares,
                                    TotalPostComments,
                                    OrganicVideoViewsLifetime,
                                    LinkClicksLifetime,
                                    UniquePostCommentsLifetime
                                FROM datif_pz_uk_{env}.03_transformed.facebook_organic_scd2_view
                                """)

window_spec = Window.partitionBy("PostID").orderBy("Date", "PostTotalImpressionsLifetime")

metrics = [
    "PostTotalImpressionsLifetime",
    "TotalPostLikes",
    "TotalLikeReactions",
    "TotalAngerReactions",
    "TotalhahaReactions",
    "TotalLoveReactions",
    "TotalSadReactions",
    "TotalwowReactions",
    "TotalPostShares",
    "TotalPostComments",
    "OrganicVideoViewsLifetime",
    "LinkClicksLifetime",
    "UniquePostCommentsLifetime"
]

df_facebook = df_facebook_daily
for metric in metrics:
    df_facebook = df_facebook.withColumn(f"{metric}Diff", F.coalesce(F.col(metric) - F.lag(metric).over(window_spec), F.col(metric)))

# Rearrange columns and rename original columns to have 'Sum' suffix
columns = []
for metric in metrics:
    columns.append(F.col(metric).alias(f"{metric}Sum"))
    columns.append(F.col(f"{metric}Diff").alias(metric))

df_facebook = df_facebook.withColumn(
    "Engagement",
    F.round(
        F.col("TotalLikeReactionsDiff") +
        F.col("TotalAngerReactionsDiff") +
        F.col("TotalhahaReactionsDiff") +
        F.col("TotalLoveReactionsDiff") +
        F.col("TotalSadReactionsDiff") +
        F.col("TotalwowReactionsDiff") +
        F.col("TotalPostSharesDiff") +
        F.col("TotalPostCommentsDiff") +
        F.col("LinkClicksLifetimeDiff"),
        4
    )
    ).withColumn(
        "EngagementRating",
        F.coalesce(
            F.round(
                (
                    0.4 * F.col("TotalPostCommentsDiff") +
                    0.3 * F.col("LinkClicksLifetimeDiff") +
                    0.2 * (
                        F.col("TotalLikeReactionsDiff") +
                        F.col("TotalAngerReactionsDiff") +
                        F.col("TotalhahaReactionsDiff") +
                        F.col("TotalLoveReactionsDiff") +
                        F.col("TotalSadReactionsDiff") +
                        F.col("TotalwowReactionsDiff")
                    ) +
                    0.1 * F.col("TotalPostSharesDiff")
                ) / F.when(F.col("PostTotalImpressionsLifetimeDiff") != 0, F.col("PostTotalImpressionsLifetimeDiff")),
                4
            ),
            F.lit(0)
        )
    ).withColumn(
        "WeightedEngagements",
        F.round(
            0.4 * F.col("TotalPostCommentsDiff") +
            0.3 * F.col("LinkClicksLifetimeDiff") +
            0.2 * (
                F.col("TotalLikeReactionsDiff") +
                F.col("TotalAngerReactionsDiff") +
                F.col("TotalhahaReactionsDiff") +
                F.col("TotalLoveReactionsDiff") +
                F.col("TotalSadReactionsDiff") +
                F.col("TotalwowReactionsDiff")
            ) +
            0.1 * F.col("TotalPostSharesDiff"),
            2
        )
    ).withColumn(
        "TotalReactionsDiff",
        F.col("TotalLikeReactionsDiff") +
        F.col("TotalAngerReactionsDiff") +
        F.col("TotalhahaReactionsDiff") +
        F.col("TotalLoveReactionsDiff") +
        F.col("TotalSadReactionsDiff") + 
        F.col("TotalwowReactionsDiff")
    )



# df_facebook = df_facebook.select("Date", "PostID", "CreatedDate", "PostMessage", "PostURL", "PostType", "Engagement", "EngagementRating", "WeightedEngagements", "TotalReactionsDiff", "TotalPostCommentsDiff", "TotalPostSharesDiff", "LinkClicksLifetimeDiff", *columns)                         


# fn_overwrite_table(df_source=df_facebook, target_schema_name=pz_target_schema_name, target_table_name="facebook_organic_daily", target_path=target_path)

# COMMAND ----------

df_facebook = (
    df_facebook
    .groupBy("Date", "PostID")
    .agg(
        *[
            F.first("CreatedDate").alias("CreatedDate"),
            F.first("PostMessage").alias("PostMessage"),
            F.first("PostURL").alias("PostURL"),
            F.first("PostType").alias("PostType"),
            # F.sum("PostTotalImpressionsLifetime").alias("PostTotalImpressionsLifetime"),
            # F.sum("TotalPostLikes").alias("TotalPostLikes"),
            # F.sum("TotalLikeReactions").alias("TotalLikeReactions"),
            # F.sum("TotalAngerReactions").alias("TotalAngerReactions"),
            # F.sum("TotalhahaReactions").alias("TotalhahaReactions"),
            # F.sum("TotalLoveReactions").alias("TotalLoveReactions"),
            # F.sum("TotalSadReactions").alias("TotalSadReactions"),
            # F.sum("TotalwowReactions").alias("TotalwowReactions"),
            # F.sum("TotalPostShares").alias("TotalPostShares"),
            # F.sum("TotalPostComments").alias("TotalPostComments"),
            # F.sum("OrganicVideoViewsLifetime").alias("OrganicVideoViewsLifetime"),
            # F.sum("LinkClicksLifetime").alias("LinkClicksLifetime"),
            # F.sum("UniquePostCommentsLifetime").alias("UniquePostCommentsLifetime"),
            F.sum("PostTotalImpressionsLifetimeDiff").alias("PostTotalImpressionsLifetime"),
            F.sum("TotalPostLikesDiff").alias("TotalPostLikesDiff"),
            F.sum("TotalLikeReactionsDiff").alias("TotalLikeReactionsDiff"),
            F.sum("TotalAngerReactionsDiff").alias("TotalAngerReactionsDiff"),
            F.sum("TotalhahaReactionsDiff").alias("TotalhahaReactionsDiff"),
            F.sum("TotalLoveReactionsDiff").alias("TotalLoveReactionsDiff"),
            F.sum("TotalSadReactionsDiff").alias("TotalSadReactionsDiff"),
            F.sum("TotalwowReactionsDiff").alias("TotalwowReactionsDiff"),
            F.sum("TotalPostSharesDiff").alias("TotalPostSharesDiff"),
            F.sum("TotalPostCommentsDiff").alias("TotalPostCommentsDiff"),
            F.sum("OrganicVideoViewsLifetimeDiff").alias("OrganicVideoViewsLifetimeDiff"),
            F.sum("LinkClicksLifetimeDiff").alias("LinkClicksLifetimeDiff"),
            F.sum("UniquePostCommentsLifetimeDiff").alias("UniquePostCommentsLifetimeDiff"),
            F.sum("WeightedEngagements").alias("WeightedEngagements"),
            F.sum("EngagementRating").alias("EngagementRating"),
            F.sum("Engagement").alias("Engagement"),
            F.sum("TotalReactionsDiff").alias("TotalReactionsDiff")
        ]
    )
)


# COMMAND ----------

fn_overwrite_table(df_source=df_facebook, target_schema_name=pz_target_schema_name, target_table_name="facebook_organic_daily", target_path=target_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instagram Organic Post

# COMMAND ----------

# load data from PZ-03-transformed into dataframe
df_instagram_post_daily = spark.sql(f"""
                               SELECT 
                                    PostID,
                                    Date,
                                    CreatedDate,
                                    PostMessage,
                                    PostURL,
                                    PostType,
                                    TotalViews AS TotalImpressions,
                                    TotalLikes,
                                    TotalShares,
                                    TotalComments,
                                    TotalSaved,
                                    TotalInteractions
                                FROM datif_pz_uk_{env}.03_transformed.instagram_organic_scd2_view
                                """)

window_spec = Window.partitionBy("PostID").orderBy("Date", "TotalImpressions")

metrics = [
    "TotalImpressions",
    "TotalLikes",
    "TotalShares",
    "TotalComments",
    "TotalSaved",
    "TotalInteractions"
]

for metric in metrics:
    df_instagram_post_daily = df_instagram_post_daily.withColumn(f"{metric}Diff", F.coalesce(F.col(metric) - F.lag(metric).over(window_spec), F.col(metric)))

# Rearrange columns
columns = []
for metric in metrics:
    columns.append(F.col(metric).alias(f"{metric}Sum"))
    columns.append(F.col(f"{metric}Diff").alias(metric))

df_instagram_post_daily = df_instagram_post_daily.withColumn(
    "Engagement",
    F.round(
        F.col("TotalLikesDiff") +
        F.col("TotalSharesDiff") +
        F.col("TotalCommentsDiff") +
        F.col("TotalSavedDiff"), 4
    )
    ).withColumn(
        "WeightedEngagements",
        F.round(
            0.4 * F.col("TotalCommentsDiff") +
            0.3 * F.col("TotalSavedDiff") +
            0.2 * F.col("TotalLikesDiff") +
            0.1 * F.col("TotalSharesDiff"), 2
        )
    ).withColumn(
        "EngagementRating",
        F.coalesce(
            F.round(
                (
                    0.4 * F.col("TotalCommentsDiff") +
                    0.3 * F.col("TotalSavedDiff") +
                    0.2 * F.col("TotalLikesDiff") +
                    0.1 * F.col("TotalSharesDiff")
                ) / F.when(F.col("TotalImpressionsDiff") != 0, F.col("TotalImpressionsDiff")).otherwise(None), 2
            ),
            F.lit(0)
        )
    )

# df_instagram_post_daily = df_instagram_post_daily.select("Date", "PostID", "CreatedDate", "PostMessage", "PostURL", "PostType", "Engagement", "EngagementRating", "WeightedEngagements", "TotalInteractionsDiff", "TotalLikesDiff", "TotalCommentsDiff", "TotalSharesDiff", "TotalSavedDiff", "TotalImpressionsDiff", *columns)


# fn_overwrite_table(df_source=df_instagram_post_daily, target_schema_name=pz_target_schema_name, target_table_name="instagram_organic_post_daily", target_path=target_path)

# COMMAND ----------

df_instagram_post_daily = (
    df_instagram_post_daily
    .groupBy("Date", "PostID")
    .agg(
        *[
            F.first("CreatedDate").alias("CreatedDate"),
            F.first("PostMessage").alias("PostMessage"),
            F.first("PostURL").alias("PostURL"),
            F.first("PostType").alias("PostType"),
            F.sum("TotalImpressionsDiff").alias("TotalImpressions"),
            F.sum("TotalLikesDiff").alias("TotalLikesDiff"),
            F.sum("TotalSharesDiff").alias("TotalSharesDiff"),
            F.sum("TotalCommentsDiff").alias("TotalCommentsDiff"),
            F.sum("TotalSavedDiff").alias("TotalSavedDiff"),
            F.sum("TotalInteractionsDiff").alias("TotalInteractionsDiff"),
            F.sum("Engagement").alias("Engagement"),
            F.sum("EngagementRating").alias("EngagementRating"),
            F.sum("WeightedEngagements").alias("WeightedEngagements")
        ]
    )
)

# COMMAND ----------

fn_overwrite_table(df_source=df_instagram_post_daily, target_schema_name=pz_target_schema_name, target_table_name="instagram_organic_post_daily", target_path=target_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instagram Organic Stories

# COMMAND ----------

# load data from PZ-03_transformed into dataframe
df_instagram_stories_daily = spark.sql(f"""
                                SELECT
                                    Date,
                                    CreatedDate,
                                    StoryID,
                                    MediaType,
                                    Username,
                                    Caption,
                                    TotalImpressions,
                                    TotalReplies,
                                    TotalShares,
                                    TotalSwipesForward,
                                    TotalTapBack,
                                    TotalTabExit,
                                    TotalTapForward,
                                    TotalReach,
                                    ROUND(
                                        TotalReplies +     
                                        TotalShares, 4
                                    ) AS Engagement,
                                    COALESCE(
                                        ROUND(
                                            (
                                                0.8 * TotalReplies +
                                                0.2 * TotalShares) / NULLIF(TotalReach, 0), 4     
                                        ),
                                        0
                                    ) AS EngagementRating,
                                    round(
                                        0.8 * TotalReplies +
                                        0.2 * TotalShares,
                                        2
                                    ) AS WeightedEngagements
                                FROM datif_pz_uk_{env}.03_transformed.instagram_organic_stories_scd2_view
                              """)

window_spec = Window.partitionBy("StoryID").orderBy("Date", "TotalImpressions")

metrics = [
    "TotalImpressions",
    "TotalReplies",
    "TotalShares",
    "TotalSwipesForward",
    "TotalTapBack",
    "TotalTabExit",
    "TotalTapForward",
    "TotalReach"
]

for metric in metrics:
    df_instagram_stories_daily = df_instagram_stories_daily.withColumn(f"{metric}Diff", F.coalesce(F.col(metric) - F.lag(metric).over(window_spec), F.col(metric)))

# Rearrange columns
columns = []
for metric in metrics:
    # columns.append(metric)
    # columns.append(f"{metric}Diff")
    columns.append(F.col(metric).alias(f"{metric}Sum"))
    columns.append(F.col(f"{metric}Diff").alias(metric))

df_instagram_stories_daily = df_instagram_stories_daily.withColumn(
    "Engagement",
    F.round(F.col("TotalRepliesDiff") + F.col("TotalSharesDiff"), 4)
    ).withColumn(
        "WeightedEngagements",
        F.round(
            0.8 * F.col("TotalRepliesDiff") +
            0.2 * F.col("TotalSharesDiff"), 2
        )
    ).withColumn(
        "EngagementRating",
        F.coalesce(
            F.round(
                (
                    0.8 * F.col("TotalRepliesDiff") +
                    0.2 * F.col("TotalSharesDiff")
                ) / F.when(F.col("TotalReachDiff") != 0, F.col("TotalReachDiff")).otherwise(None), 4
            ),
            F.lit(0)
        )
    )

df_instagram_stories_daily = df_instagram_stories_daily.select("Date", "CreatedDate", "StoryID", "MediaType", "Username", "Caption", "Engagement", "EngagementRating" , "WeightedEngagements", "TotalRepliesDiff", "TotalSharesDiff", "TotalReachDiff", *columns)

fn_overwrite_table(df_source=df_instagram_stories_daily, target_schema_name=pz_target_schema_name, target_table_name="instagram_organic_stories_daily", target_path=target_path)
