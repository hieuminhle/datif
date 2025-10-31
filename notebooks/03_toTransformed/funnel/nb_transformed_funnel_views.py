# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Daily Export
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der FUnnel API abgezogenen Daten der Social Media Daten wurden in der DZ unter dem Schema 02_cleaned für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC - 02_cleaned_uk_facebook.001_fb_organic_posts_insights_scd2_view
# MAGIC - 02_cleaned_uk_instagram.002_ig_organic_post_scd2_view
# MAGIC - 02_cleaned_uk_instagram.ig_stories_adverity_scd2_view
# MAGIC - 02_cleaned_uk_x.003_x_organic_post_current_view
# MAGIC - 02_cleaned_uk_linkedin.004_li_organic_post_current_view
# MAGIC - 02_cleaned_uk_linkedin.005_li_organic_video_current_view
# MAGIC - 02_cleaned_uk_linkedin.007_youtube_organic_post_current_view
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 03_transformed.facebook_organic_scd2_view
# MAGIC - 03_transformed.instagram_organic_scd2_view
# MAGIC - 03_transformed.instagram_organic_stories_scd2_view
# MAGIC - 03_transformed.x_organic_daily_view
# MAGIC - 03_transformed.linkedin_organic_post_daily_view
# MAGIC - 03_transformed.linkedin_organic_video_daily_view
# MAGIC - 03_transformed.linkedin_organic_total_daily_view
# MAGIC - 03_transformed.youtube_organic_post_daily_view
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 22.10.2025 Minh Hieu Le: Korrektur der Formel von Engagement und WeightedEngagement für YouTube 
# MAGIC - 16.10.2025 Minh Hieu Le: Korrektur der Formel von Engagement und WeightedEngagement für YouTube (TotalViews statt daily Views im Nenner)
# MAGIC - 10.10.2025 Minh Hieu Le: Anpassen, sodass bei LinkedIn die Zahlenspalten von numerischen Datentypen sind
# MAGIC - 07.10.2025 Minh Hieu Le: Anpassen, sodass bei YouTube bei Divisionen keine Null im Nenner ist
# MAGIC - 16.07.2025 Justin Stange-Heiduk: Add datif_pz_uk_{env}.
# MAGIC - 25.06.2025 JUstin Stange-Heiduk: Youtube Owner Spalte hinzugefügt
# MAGIC - 11.06.2025 Justin Stange-Heiduk: Add EngagementRating und WeightedEngagamenet to not Meta Channel
# MAGIC - 13.05.2025 Justin Stange-Heiduk: Verbesserte Konsolidierte LinkedIn-Daten aus Posts & Videos
# MAGIC - 07.05.2025 Justin Stange-Heiduk: Instagram API Änderung Impression -> Views
# MAGIC - 06.05.2025 Minh Hieu Le: VideoURL hinzugefügt
# MAGIC - 23.04.2025 Philipp Sandhaas: konsolidierte LinkedIn-View
# MAGIC - 13.02.2025 Svenja Schuder: Schema Updates
# MAGIC - 06.02.2025 Svenja Schuder: Unity Catalog als Variable hinzugefügt
# MAGIC - 03.12.2024 Svenja Schuder: Schemabereinigungen

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facebook Organic Posts

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.facebook_organic_scd2_view AS
          SELECT
            --DATE_FORMAT(CAST(VALID_FROM AS DATE), 'yyyy-MM-dd') as Date,
            CAST(DATE_FORMAT(VALID_FROM, 'yyyy-MM-dd') as DATE) as Date,
            Post_ID__Facebook_Pages as PostID,
            CAST(DATE_FORMAT(Created_Time__Facebook_Pages, 'yyyy-MM-dd') AS DATE) as CreatedDate,
            Post_Message__Facebook_Pages as PostMessage,
            Post_Type__Facebook_Pages as PostType,
            Post_Permalink_URL__Facebook_Pages as PostURL,
            Post_URL__Facebook_Pages as ContentURL,
            CAST(Post_Total_Impressions_Lifetime__Facebook_Pages AS INTEGER) as PostTotalImpressionsLifetime,
            CAST(Post_Likes__Facebook_Pages AS INTEGER) as TotalPostLikes,
            CAST(Total_Like_Reactions_of_a_post__Lifetime__Facebook_Pages AS INTEGER) as TotalLikeReactions,
            CAST(Total_anger_Reactions_of_a_post__Lifetime__Facebook_Pages AS INTEGER) as TotalAngerReactions,
            CAST(Total_haha_Reactions_of_a_post__Lifetime__Facebook_Pages AS INTEGER) as TotalhahaReactions,
            CAST(Total_Love_Reactions_of_a_post__Lifetime__Facebook_Pages AS INTEGER) as TotalLoveReactions,
            CAST(Total_sad_Reactions_of_a_post__Lifetime__Facebook_Pages AS INTEGER) as TotalSadReactions,
            CAST(Total_wow_Reactions_of_a_post__Lifetime__Facebook_Pages AS INTEGER) as TotalwowReactions,
            CAST(Post_Shares__Facebook_Pages AS INTEGER) as TotalPostShares,
            CAST(Post_Comments__Facebook_Pages AS INTEGER) as TotalPostComments,
            CAST(Organic_Video_Views_Lifetime__Facebook_Pages AS INTEGER) as OrganicVideoViewsLifetime,
            CAST(Page_Post_Link_Clicks_Lifetime__Facebook_Pages AS INTEGER) as LinkClicksLifetime,
            CAST(Unique_Post_Comments___Lifetime_Post__Facebook_Pages AS INTEGER) as UniquePostCommentsLifetime            
          FROM datif_dz_{env}.02_cleaned_uk_facebook.001_fb_organic_posts_insights_scd2_view
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instagram Organic Post

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.instagram_organic_scd2_view AS
          SELECT
            CAST(DATE_FORMAT(VALID_FROM, 'yyyy-MM-dd') as DATE) as Date,
            CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') AS DATE) as CreatedDate,
            Media_ID__Instagram_Insights as PostID,
            Caption__Instagram_Insights as PostMessage,
            Permalink__Instagram_Insights as PostURL,
            Media_type__Instagram_Insights as PostType,
            CAST(
              CASE 
                  WHEN Views__Instagram_Insights IS NULL OR Views__Instagram_Insights = 0 THEN
                      CASE 
                          WHEN Plays__Instagram_Insights IS NULL OR Plays__Instagram_Insights = 0 THEN 
                              Impressions__Instagram_Insights
                          ELSE 
                              Plays__Instagram_Insights
                      END
                  ELSE 
                      Views__Instagram_Insights
              END 
            AS INTEGER) AS TotalViews,
            CAST(Likes__Instagram_Insights AS INTEGER) as TotalLikes,
            CAST(Shares__Instagram_Insights AS INTEGER) as TotalShares,
            CAST(Comments__Instagram_Insights AS INTEGER) as TotalComments,
            CAST(Saved__Instagram_Insights AS INTEGER) as TotalSaved,
            CAST(Total_Interactions__Instagram_Insights AS INTEGER) as TotalInteractions
          FROM datif_dz_{env}.02_cleaned_uk_instagram.002_ig_organic_post_scd2_view
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instagram Stories

# COMMAND ----------

# spark.sql(f"""
#           CREATE OR REPLACE VIEW 03_transformed.instagram_organic_stories_scd2_view AS
#           SELECT
#             to_date(CAST(VALID_FROM as DATE)) as Date,
#             CAST(Date AS DATE) as CreatedDate,
#             Story_ID__Instagram_Stories as StoryID,
#             Media_Type__Instagram_Stories as MediaType,
#             Username__Instagram_Stories as Username,
#             Caption__Instagram_Stories as Caption,
#             coalesce(CAST(Impressions__Instagram_Stories AS INTEGER),0) as TotalImpressions,
#             -- CAST(Views__Instagram_Insights AS INTEGER) as TotalViews,
#             CAST(Replies__Instagram_Stories AS INTEGER) as TotalReplies,
#             CAST(Shares__Instagram_Stories AS INTEGER) as TotalShares,
#             CAST(Swipe_Forward__Instagram_Stories AS INTEGER) as TotalSwipesForward,
#             CAST(Tap_Back__Instagram_Stories AS INTEGER) as TotalTapBack,
#             CAST(Tap_Exit__Instagram_Stories AS INTEGER) as TotalTabExit,
#             CAST(Tap_Forward__Instagram_Stories AS INTEGER) as TotalTapForward,
#             CAST(Reach__Instagram_Stories AS INTEGER) as TotalReach
#           FROM datif_dz_{env}.02_cleaned_uk_instagram.011_ig_organic_stories_scd2_view
#           """)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.instagram_organic_stories_scd2_view AS
    SELECT
        to_date(CAST(VALID_FROM as DATE)) as Date,
        CAST(Date AS DATE) as CreatedDate,
        Story_ID__Instagram_Stories as StoryID,
        Media_Type__Instagram_Stories as MediaType,
        Username__Instagram_Stories as Username,
        Caption__Instagram_Stories as Caption,
        CASE
            WHEN Impressions__Instagram_Stories IS NULL OR CAST(Impressions__Instagram_Stories AS INTEGER) = 0 THEN
                CASE
                    WHEN Views__Instagram_Stories IS NULL OR CAST(Views__Instagram_Stories AS INTEGER) = 0 THEN
                        CAST(Reach__Instagram_Stories AS INTEGER)
                    ELSE
                        CAST(Views__Instagram_Stories AS INTEGER)
                END
            ELSE
                CAST(Impressions__Instagram_Stories AS INTEGER)
        END AS TotalImpressions,
        CAST(Replies__Instagram_Stories AS INTEGER) as TotalReplies,
        CAST(Shares__Instagram_Stories AS INTEGER) as TotalShares,
        CAST(Swipe_Forward__Instagram_Stories AS INTEGER) as TotalSwipesForward,
        CAST(Tap_Back__Instagram_Stories AS INTEGER) as TotalTapBack,
        CAST(Tap_Exit__Instagram_Stories AS INTEGER) as TotalTabExit,
        CAST(Tap_Forward__Instagram_Stories AS INTEGER) as TotalTapForward,
        CAST(Reach__Instagram_Stories AS INTEGER) as TotalReach,
        CAST(Views__Instagram_Stories AS INTEGER) as TotalViews

    FROM datif_dz_{env}.02_cleaned_uk_instagram.011_ig_organic_stories_scd2_view
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## X Organic Post

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.x_organic_daily_view AS
    SELECT
        CAST(x.Date AS DATE) AS Date,
        x.Post_ID__X_Organic AS PostID,
        xt.CreatedDate AS CreatedDate,
        x.In_Reply_To_Status_ID__X_Organic AS InReplyToStatusID,
        x.Post_Text__X_Organic AS PostMessage,
        CONCAT('https://x.com/EnBW/status/', x.Post_ID__X_Organic) AS PostURL,
        x.Post_Media_Type__X_Organic AS PostType,
        COALESCE(CAST(x.Impressions__X_Organic AS INTEGER), 0) AS Impressions,
        COALESCE(CAST(x.Likes__X_Organic AS INTEGER), 0) AS Likes,
        COALESCE(CAST(x.Reposts__X_Organic AS INTEGER), 0) AS Reposts,
        COALESCE(CAST(x.Replies__X_Organic AS INTEGER), 0) AS Replies,
        COALESCE(CAST(x.Follows__X_Organic AS INTEGER), 0) AS Follows,
        COALESCE(CAST(x.Clicks__X_Organic AS INTEGER), 0) AS Clicks,
        COALESCE(CAST(x.Link_Clicks__X_Organic AS INTEGER), 0) AS LinkClicks,
        COALESCE(CAST(x.App_Clicks__X_Organic AS INTEGER), 0) AS AppClicks,
        COALESCE(CAST(x.Card_Engagements__X_Organic AS INTEGER), 0) AS CardEngagements,
        ROUND(
            CASE
                WHEN COALESCE(x.Impressions__X_Organic, 0) = 0 THEN 0
                ELSE (
                    ((COALESCE(x.Likes__X_Organic, 0) + COALESCE(x.Clicks__X_Organic, 0)) * 0.35 +
                    COALESCE(x.Reposts__X_Organic, 0) * 0.2 +
                    COALESCE(x.Replies__X_Organic, 0) * 0.45)
                    / COALESCE(x.Impressions__X_Organic, 1)
                )
            END, 4
        ) AS EngagementRateInPercent,
        ROUND(
            (COALESCE(x.Likes__X_Organic, 0) + COALESCE(x.Clicks__X_Organic, 0)) * 0.35 +
            COALESCE(x.Reposts__X_Organic, 0) * 0.2 +
            COALESCE(x.Replies__X_Organic, 0) * 0.45,
            2
        ) AS WeightedEngagements,
        ROUND(
            COALESCE(x.Likes__X_Organic, 0) +
            COALESCE(x.Clicks__X_Organic, 0) +
            COALESCE(x.Reposts__X_Organic, 0) +
            COALESCE(x.Replies__X_Organic, 0),
            2
        ) AS Engagements
    FROM datif_dz_{env}.02_cleaned_uk_x.003_x_organic_post_current_view x
    LEFT JOIN datif_pz_uk_{env}.`03_transformed`.x_organic_total xt
        ON x.Post_ID__X_Organic = xt.PostID
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LinkedIn Organic Consolidated

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.linkedin_organic_daily_view AS

SELECT 
    Post_URN_ID__LinkedIn_Organic AS PostID,
    CAST(Date AS DATE) AS Date,
    CAST(Posted_Date__LinkedIn_Organic AS DATE) AS CreatedDate,
    CASE 
        WHEN Post_Content__LinkedIn_Organic IS NULL OR TRIM(Post_Content__LinkedIn_Organic) = '' 
            THEN 'Keine Postbeschreibung verfügbar'
        ELSE Post_Content__LinkedIn_Organic 
    END as PostContent,
    Post_link__LinkedIn_Organic AS PostURL,
    CASE
        WHEN Content_Type__LinkedIn_Organic = 'Video' THEN 'Image'
        ELSE Content_Type__LinkedIn_Organic
    END AS ContentType,
    COALESCE(CAST(Impressions__LinkedIn_Organic AS INTEGER), 0) AS Impressions,
    COALESCE(CAST(Likes__LinkedIn_Organic AS INTEGER), 0) AS Likes,
    COALESCE(CAST(Shares__LinkedIn_Organic AS INTEGER), 0) AS Shares,
    COALESCE(CAST(Comments__LinkedIn_Organic AS INTEGER), 0) AS Comments,
    0 AS Views,
    COALESCE(CAST(Clicks__LinkedIn_Organic AS INTEGER), 0) AS Clicks,
    0 AS Viewers,
    0.0 AS TimeWatchedForVideoViews,
    0.0 AS TimeWatched,
    ROUND(
        CASE
            WHEN COALESCE(CAST(Impressions__LinkedIn_Organic AS INTEGER), 0) = 0 THEN 0
            ELSE(
                (COALESCE(CAST(Likes__LinkedIn_Organic AS INTEGER), 0) * 0.1 +
                 COALESCE(CAST(Shares__LinkedIn_Organic AS INTEGER), 0) * 0.3 +
                 COALESCE(CAST(Comments__LinkedIn_Organic AS INTEGER), 0) * 0.4 +
                 COALESCE(CAST(Clicks__LinkedIn_Organic AS INTEGER), 0) * 0.2) 
                / COALESCE(CAST(Impressions__LinkedIn_Organic AS INTEGER), 1) 
            )
        END, 4
    ) AS EngagementRateInPercent,
    ROUND(
        (COALESCE(CAST(Likes__LinkedIn_Organic AS INTEGER), 0) * 0.1 +
        COALESCE(CAST(Shares__LinkedIn_Organic AS INTEGER), 0) * 0.3 +
        COALESCE(CAST(Comments__LinkedIn_Organic AS INTEGER), 0) * 0.4 +
        COALESCE(CAST(Clicks__LinkedIn_Organic AS INTEGER), 0) * 0.2),
        2        
    ) AS WeightedEngagements,
    (   COALESCE(CAST(Likes__LinkedIn_Organic AS INTEGER), 0) +  
        COALESCE(CAST(Shares__LinkedIn_Organic AS INTEGER), 0) + 
        COALESCE(CAST(Comments__LinkedIn_Organic AS INTEGER), 0) + 
        COALESCE(CAST(Clicks__LinkedIn_Organic AS INTEGER), 0)
    ) AS Engagements,
    'Post' AS Source

FROM datif_dz_{env}.02_cleaned_uk_linkedin.004_li_organic_post_current_view

UNION ALL

SELECT
    Video_ID__LinkedIn_Organic AS PostID,
    CAST(Date AS DATE) AS Date,
    CAST(Created_Time__LinkedIn_Organic AS DATE) AS CreatedDate,
    CASE 
        WHEN Video_Text__LinkedIn_Organic IS NULL OR TRIM(Video_Text__LinkedIn_Organic) = '' 
            THEN 'Keine Videobeschreibung verfügbar'
        ELSE Video_Text__LinkedIn_Organic 
    END as PostContent,
    Video_Post_URL__LinkedIn_Organic AS PostURL,
    Content_Type__LinkedIn_Organic AS ContentType,
    COALESCE(CAST(Impressions_Video__LinkedIn_Organic AS INTEGER), 0) AS Impressions,
    COALESCE(CAST(Likes_Video__LinkedIn_Organic AS INTEGER), 0) AS Likes,
    COALESCE(CAST(Shares_Video__LinkedIn_Organic AS INTEGER), 0) AS Shares,
    COALESCE(CAST(Comments_Video__LinkedIn_Organic AS INTEGER), 0) AS Comments,
    COALESCE(CAST(Views_Video__LinkedIn_Organic AS INTEGER), 0) AS Views,
    COALESCE(CAST(Clicks_Video__LinkedIn_Organic AS INTEGER), 0) AS Clicks,
    COALESCE(CAST(Viewers__LinkedIn_Organic AS INTEGER), 0) AS Viewers,
    ROUND(COALESCE(CAST(Time_watched_for_video_views__LinkedIn_Organic AS DOUBLE), 0.0), 2) AS TimeWatchedForVideoViews,
    ROUND(COALESCE(CAST(Time_Watched__LinkedIn_Organic AS DOUBLE), 0.0), 2) AS TimeWatched,
    ROUND(
        CASE
            WHEN COALESCE(CAST(Impressions_Video__LinkedIn_Organic AS INTEGER), 0) = 0 THEN 0
            ELSE (
                (COALESCE(CAST(Likes_Video__LinkedIn_Organic AS INTEGER), 0) * 0.1 +
                 COALESCE(CAST(Shares_Video__LinkedIn_Organic AS INTEGER), 0) * 0.3 +
                 COALESCE(CAST(Comments_Video__LinkedIn_Organic AS INTEGER), 0) * 0.4 +
                 COALESCE(CAST(Clicks_Video__LinkedIn_Organic AS INTEGER), 0) * 0.2) 
                / COALESCE(CAST(Impressions_Video__LinkedIn_Organic AS INTEGER), 1)
            )
        END, 4
    ) AS EngagementRateInPercent,
    ROUND(
        COALESCE(CAST(Likes_Video__LinkedIn_Organic AS INTEGER), 0) * 0.1 +
        COALESCE(CAST(Shares_Video__LinkedIn_Organic AS INTEGER), 0) * 0.3 +
        COALESCE(CAST(Comments_Video__LinkedIn_Organic AS INTEGER), 0) * 0.4 +
        COALESCE(CAST(Clicks_Video__LinkedIn_Organic AS INTEGER), 0) * 0.2,
        2        
    ) AS WeightedEngagements,
    (
        COALESCE(CAST(Likes_Video__LinkedIn_Organic AS INTEGER), 0) + 
        COALESCE(CAST(Shares_Video__LinkedIn_Organic AS INTEGER), 0) + 
        COALESCE(CAST(Comments_Video__LinkedIn_Organic AS INTEGER), 0) + 
        COALESCE(CAST(Clicks_Video__LinkedIn_Organic AS INTEGER), 0)
    ) AS Engagements,
    'Video' AS Source

FROM datif_dz_{env}.02_cleaned_uk_linkedin.005_li_organic_video_current_view
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Youtube Organic Post

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.youtube_organic_post_daily_view AS
    SELECT
        CAST(yd.Date AS DATE) as Date,
        CAST(yd.Published_At__YouTube AS DATE) as CreatedDate,
        yd.Video_ID__YouTube as VideoID,
        yd.Channel_ID__YouTube as ChannelID,
        yd.Video_Title__YouTube as VideoTitle,
        concat('https://www.youtube.com/watch?v=', yd.Video_ID__YouTube) as VideoURL,
        yd.Tags__YouTube as Tags,
        CASE 
            WHEN yd.Video_Description__YouTube IS NULL OR TRIM(yd.Video_Description__YouTube) = '' 
                THEN 'Keine Videobeschreibung verfügbar'
            ELSE yd.Video_Description__YouTube 
        END as VideoDescription,
        yd.Duration_HH_MM_SS__YouTube as Duration,
        CAST(yd.Views__YouTube AS INTEGER) as Views,
        CAST(yd.Likes__YouTube AS INTEGER) as Likes,
        CAST(yd.Dislikes__YouTube AS INTEGER) as Dislikes,
        CAST(yd.Comments__YouTube AS INTEGER) as Comments,
        CAST(yd.Shares__YouTube AS INTEGER) as Shares,
        CAST(yd.Subscribers_Gained__YouTube AS INTEGER) as SubscribersGained,
        CAST(yd.Subscribers_Lost__YouTube AS INTEGER) as SubscribersLost,
        CAST(yd.Card_Impressions__YouTube AS INTEGER) as CardImpressions,
        CAST(yd.Card_Teaser_Impressions__YouTube AS INTEGER) as CardTeaserImpressions,
        CAST(yd.Annotation_Clickable_Impressions__YouTube AS INTEGER) as AnnotationClickableImpressions,
        CAST(yd.Annotation_Closable_Impressions__YouTube AS INTEGER) as AnnotationClosableImpressions,
        CAST(yd.Estimated_Minutes_Watched__YouTube AS DOUBLE) as EstimatedMinutesWatched,
        CAST(yd.Views_YouTube_Premium__YouTube AS INTEGER) as ViewsPremium,
        CAST(yd.Annotation_Clicks__YouTube AS INTEGER) as AnnotationClicks,

        ROUND(
            CASE
                WHEN COALESCE(yt.TotalViews, 0) = 0 THEN 0
                ELSE try_divide(CAST(yd.Estimated_Minutes_Watched__YouTube AS DOUBLE)*60 , CAST(yt.TotalViews AS DOUBLE))
            END, 2
        ) as TotalAverageViewDuration,

        ROUND(
            CASE
                WHEN COALESCE(yt.TotalViews, 0) = 0 THEN 0
                ELSE try_divide(COALESCE(yd.Estimated_Minutes_Watched__YouTube, 0) , CAST(yt.TotalViews AS DOUBLE))
            END, 2
        ) AS AverageMinutesWatched,

        ROUND(
            CASE
                WHEN COALESCE(yd.Views__YouTube, 0) = 0 THEN 0
                ELSE (
                    try_divide((COALESCE(yd.Likes__YouTube, 0) * 0.2 +
                     COALESCE(yd.Dislikes__YouTube, 0) * 0.05 +
                     COALESCE(yd.Comments__YouTube, 0) * 0.25 +
                     COALESCE(yd.Shares__YouTube, 0) * 0.15 +
                     COALESCE(
                        try_divide(COALESCE(yd.Estimated_Minutes_Watched__YouTube, 0) , COALESCE(yd.Views__YouTube, 1)),
                        0
                     ) * 0.35)
                    , COALESCE(yd.Views__YouTube, 1))
                )
            END, 4
        ) AS EngagementRateInPercent,

        COALESCE(ROUND(
            COALESCE(yd.Likes__YouTube, 0) * 0.2 +
            COALESCE(yd.Dislikes__YouTube, 0) * 0.05 +
            COALESCE(yd.Comments__YouTube, 0) * 0.25 +
            COALESCE(yd.Shares__YouTube, 0) * 0.15 +
            (CASE
                WHEN COALESCE(yt.TotalViews, 0) = 0 THEN 0
                ELSE try_divide(COALESCE(yd.Estimated_Minutes_Watched__YouTube, 0) , CAST(yt.TotalViews AS DOUBLE))
            END) * 0.35, 2        
        ), 0) AS WeightedEngagements,
        ROUND(
            COALESCE(yd.Likes__YouTube, 0) +
            COALESCE(yd.Dislikes__YouTube, 0) +
            COALESCE(yd.Comments__YouTube, 0) +
            COALESCE(yd.Shares__YouTube, 0) +
            (CASE
                WHEN COALESCE(yt.TotalViews, 0) = 0 THEN 0
                ELSE try_divide(COALESCE(yd.Estimated_Minutes_Watched__YouTube, 0) , CAST(yt.TotalViews AS DOUBLE))
            END), 2
        ) AS Engagements,
        yt.Owner as Owner

    FROM datif_dz_{env}.02_cleaned_uk_yt.007_yt_organic_post_current_view yd
    LEFT JOIN datif_pz_uk_{env}.03_transformed.youtube_organic_post_total yt
        ON yd.Video_ID__YouTube = yt.VideoID
""")

