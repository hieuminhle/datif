# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Total Aggregation
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der Funnel API abgezogenen Daten der Social Media Daten wurden in der DZ unter dem Schema 02_cleaned für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Total Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggerde wird das notebook über die pipeline '00-1000-Funnel-orchestrate'.
# MAGIC
# MAGIC ---
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 03_transformed.facebook_organic_total
# MAGIC - 03_transformed.instagram_organic_total
# MAGIC - 03_transformed.instagram_organic_stories_total
# MAGIC - 03_transformed.x_organic_total
# MAGIC - 03_transformed.linkedin_organic_post_total
# MAGIC - 03_transformed.linkedin_organic_video_total
# MAGIC
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 04_power_bi_mart.facebook_organic_total
# MAGIC - 04_power_bi_mart.instagram_organic_post_total
# MAGIC - 04_power_bi_mart.instagram_organic_stories_total
# MAGIC - 04_power_bi_mart.x_organic_total
# MAGIC - 04_power_bi_mart.linkedin_organic_post_total
# MAGIC - 04_power_bi_mart.linkedin_organic_video_total
# MAGIC - 04_power_bi_mart.youtube_organic_post_total
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC 16.07.2025 Justin Stange-Heiduk: Add datif_pz_uk_{env}.
# MAGIC - 25.06.2025 JUstin Stange-Heiduk: Youtube Owner Spalte hinzugefügt
# MAGIC - 27.05.2025 Justin Stange-Heiduk: Youtube Themenbereiche hinzugefügt, Performancekultur entfernt
# MAGIC - 12.05.2025 Justin Stange-Heiduk: LinkedIn Consolidated hinzufügen
# MAGIC - 07.05.2025 Justin Stange-Heiduk: Instagram API Änderung Impression -> Views
# MAGIC - 06.05.2025 Minh Hieu Le: Bei YouTube VideoURL mit selektiert
# MAGIC - 13.02.2025 Svenja Schuder: Schema Updates
# MAGIC - 06.02.2025 Svenja Schuder: Unity Catalog als Variable hinzugefügt
# MAGIC - 06.12.2024 Svenja Schuder: C-Level-Tagging hinzugefügt
# MAGIC - 03.12.2024 Svenja Schuder: Schemabereinigungen
# MAGIC - 16.12.2024 Svenja Schuder: update ig stories table   
# MAGIC - 28.11.2024 Svenja Schuder: Schemaänderung + Upsert Methode zu Overwrite geändert + instagram_stories und youtube hinzugefügt
# MAGIC - 13.11.2024 Svenja Schuder: init

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

from pyspark.sql.functions import expr, array, when, col, concat_ws, trim, regexp_replace

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Parameters

# COMMAND ----------

pz_target_schema_name = "04_power_bi_mart"
target_path = "funnel"

#strategic_areas_threshold = 0.4
strategic_areas_threshold = 0.8

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04-power-bi-mart

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facebook Organic Posts

# COMMAND ----------

df_facebook_total = spark.sql(f"""
                                SELECT 
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
                                    UniquePostCommentsLifetime,
                                    EngagementRateInPercent,
                                    CLevelErwaehnungen,
                                    TRIM(Themenbereich1) AS SubjectArea1,
                                    TRIM(Themenbereich2) AS SubjectArea2,
                                    TRIM(Themenbereich3) AS SubjectArea3,
                                    Themenbereich1_Conf AS SubjectArea1_Confidence,
                                    Themenbereich2_Conf AS SubjectArea2_Confidence,
                                    Themenbereich3_Conf AS SubjectArea3_Confidence,
                                    -- IF(Strategie2030 >= {strategic_areas_threshold}, TRUE, FALSE) AS Strategie2030,
                                    -- IF(FinanzierungEnergiewende >= {strategic_areas_threshold}, TRUE, FALSE) AS FinanzierungEnergiewende,
                                    -- IF(EMobilitaet >= {strategic_areas_threshold}, TRUE, FALSE) AS EMobilitaet,
                                    -- IF(Performancekultur >= {strategic_areas_threshold}, TRUE, FALSE) AS Performancekultur,
                                    -- IF(VernetzeEnergiewelt >= {strategic_areas_threshold}, TRUE, FALSE) AS VernetzeEnergiewelt,
                                    -- -- IF(Commodity > {strategic_areas_threshold}, TRUE, FALSE) AS Commodity,
                                    -- IF(TransformationGasnetzeWasserstoff >= {strategic_areas_threshold}, TRUE, FALSE) AS TransformationGasnetzeWasserstoff,
                                    -- IF(ErneuerbareEnergien >= {strategic_areas_threshold}, TRUE, FALSE) AS ErneuerbareEnergien,
                                    -- IF(DisponibleErzeugung >= {strategic_areas_threshold}, TRUE, FALSE) AS DisponibleErzeugung,
                                    -- IF(IntelligenteStromnetze >= {strategic_areas_threshold}, TRUE, FALSE) AS IntelligenteStromnetze,
                                    -- IF(EnBWAlsArbeitgeberIn >= {strategic_areas_threshold}, TRUE, FALSE) AS EnBWAlsArbeitgeberIn,
                                    -- IF(NachhaltigkeitCSRESG >= {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
                                    -- IF(MarkeEnBW >= {strategic_areas_threshold}, TRUE, FALSE) AS MarkeEnBW
                                    Strategie2030,
                                    FinanzierungEnergiewende,
                                    EMobilitaet,
                                    VernetzeEnergiewelt,
                                    TransformationGasnetzeWasserstoff,
                                    ErneuerbareEnergien,
                                    DisponibleErzeugung,
                                    IntelligenteStromnetze,
                                    EnBWAlsArbeitgeberIn,
                                    NachhaltigkeitCSRESG,
                                    MarkeEnBW
                                FROM datif_pz_uk_{env}.03_transformed.facebook_organic_total
                                """)

# COMMAND ----------

from pyspark.sql.functions import expr, array, when, col, concat_ws, trim, regexp_replace

# List of columns to check for true values
columns_to_check = [
    "Strategie2030", "FinanzierungEnergiewende", "EMobilitaet", 
    "VernetzeEnergiewelt", "TransformationGasnetzeWasserstoff", "ErneuerbareEnergien", 
    "DisponibleErzeugung", "IntelligenteStromnetze", "EnBWAlsArbeitgeberIn", 
    "NachhaltigkeitCSRESG", "MarkeEnBW"
]

# Create a new column 'Themen' with the column names where the value is >= 0.8
df_facebook_total = df_facebook_total.withColumn(
    "StrategischeThemen",
    concat_ws(";", *[when(col(c) >= 0.8, c) for c in columns_to_check])
)\
    .withColumn(
    "StrategischeThemen",
    when(trim(col("StrategischeThemen")) == "", "Kein strategisches Thema").otherwise(col("StrategischeThemen"))
)

df_facebook_total = df_facebook_total.select('PostID', 'CreatedDate', 'PostMessage', 'PostURL', 'PostType', 'PostTotalImpressionsLifetime', 'TotalPostLikes', 'TotalLikeReactions', 'TotalAngerReactions', 'TotalhahaReactions', 'TotalLoveReactions', 'TotalSadReactions', 'TotalwowReactions', 'TotalPostShares', 'TotalPostComments', 'OrganicVideoViewsLifetime', 'LinkClicksLifetime', 'UniquePostCommentsLifetime', 'CLevelErwaehnungen', 'SubjectArea1', 'SubjectArea2', 'SubjectArea3', 'SubjectArea1_Confidence', 'SubjectArea2_Confidence', 'SubjectArea3_Confidence', 'StrategischeThemen')


fn_overwrite_table(df_source=df_facebook_total, target_schema_name=pz_target_schema_name, target_table_name="facebook_organic_total", target_path=target_path)      

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instagram Organic Post

# COMMAND ----------

# load data from PZ-03_transformed into dataframe
df_instagram_total = spark.sql(f"""
                                SELECT 
                                    PostID,
                                    CreatedDate,                                    
                                    PostMessage,
                                    PostURL,
                                    PostType,
                                    TotalViews AS TotalImpressions,
                                    TotalLikes,
                                    TotalShares,
                                    TotalComments,
                                    TotalSaved,
                                    TotalInteractions,
                                    EngagementRateInPercent,
                                    CLevelErwaehnungen,
                                    TRIM(Themenbereich1) AS SubjectArea1,
                                    TRIM(Themenbereich2) AS SubjectArea2,
                                    TRIM(Themenbereich3) AS SubjectArea3,
                                    Themenbereich1_Conf AS SubjectArea1_Confidence,
                                    Themenbereich2_Conf AS SubjectArea2_Confidence,
                                    Themenbereich3_Conf AS SubjectArea3_Confidence,
                                    -- IF(Strategie2030 > {strategic_areas_threshold}, TRUE, FALSE) AS Strategie2030,
                                    -- IF(FinanzierungEnergiewende > {strategic_areas_threshold}, TRUE, FALSE) AS FinanzierungEnergiewende,
                                    -- IF(EMobilitaet > {strategic_areas_threshold}, TRUE, FALSE) AS EMobilitaet,
                                    -- IF(Performancekultur > {strategic_areas_threshold}, TRUE, FALSE) AS Performancekultur,
                                    -- IF(VernetzeEnergiewelt > {strategic_areas_threshold}, TRUE, FALSE) AS VernetzeEnergiewelt,
                                    -- -- IF(Commodity > {strategic_areas_threshold}, TRUE, FALSE) AS Commodity,
                                    -- IF(TransformationGasnetzeWasserstoff > {strategic_areas_threshold}, TRUE, FALSE) AS TransformationGasnetzeWasserstoff,
                                    -- IF(ErneuerbareEnergien > {strategic_areas_threshold}, TRUE, FALSE) AS ErneuerbareEnergien,
                                    -- IF(DisponibleErzeugung > {strategic_areas_threshold}, TRUE, FALSE) AS DisponibleErzeugung,
                                    -- IF(IntelligenteStromnetze > {strategic_areas_threshold}, TRUE, FALSE) AS IntelligenteStromnetze,
                                    -- IF(EnBWAlsArbeitgeberIn > {strategic_areas_threshold}, TRUE, FALSE) AS EnBWAlsArbeitgeberIn,
                                    -- IF(NachhaltigkeitCSRESG > {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
                                    -- IF(MarkeEnBW > {strategic_areas_threshold}, TRUE, FALSE) AS MarkeEnBW
                                    Strategie2030,
                                    FinanzierungEnergiewende,
                                    EMobilitaet,
                                    VernetzeEnergiewelt,
                                    TransformationGasnetzeWasserstoff,
                                    ErneuerbareEnergien,
                                    DisponibleErzeugung,
                                    IntelligenteStromnetze,
                                    EnBWAlsArbeitgeberIn,
                                    NachhaltigkeitCSRESG,
                                    MarkeEnBW
                                FROM datif_pz_uk_{env}.03_transformed.instagram_organic_total
                                """)                

# COMMAND ----------



# List of columns to check for true values
columns_to_check = [
    "Strategie2030", "FinanzierungEnergiewende", "EMobilitaet", 
    "VernetzeEnergiewelt", "TransformationGasnetzeWasserstoff", "ErneuerbareEnergien", 
    "DisponibleErzeugung", "IntelligenteStromnetze", "EnBWAlsArbeitgeberIn", 
    "NachhaltigkeitCSRESG", "MarkeEnBW"
]

# Create a new column 'Themen' with the column names where the value is >= 0.8
df_instagram_total = df_instagram_total.withColumn(
    "StrategischeThemen",
    concat_ws(";", *[when(col(c) >= 0.8, c) for c in columns_to_check])
)\
    .withColumn(
    "StrategischeThemen",
    when(trim(col("StrategischeThemen")) == "", "Kein strategisches Thema").otherwise(col("StrategischeThemen"))
)

df_instagram_total = df_instagram_total.select('PostID', 'CreatedDate', 'PostMessage', 'PostURL', 'PostType', 'TotalImpressions', 'TotalLikes', 'TotalShares', 'TotalComments', 'TotalSaved', 'TotalInteractions', 'CLevelErwaehnungen', 'SubjectArea1', 'SubjectArea2', 'SubjectArea3', 'SubjectArea1_Confidence', 'SubjectArea2_Confidence', 'SubjectArea3_Confidence', 'StrategischeThemen')


fn_overwrite_table(df_source=df_instagram_total, target_schema_name=pz_target_schema_name, target_table_name="instagram_organic_post_total", target_path=target_path)      

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instagram Organic Stories

# COMMAND ----------

# load data from PZ-03_transformed into dataframe
df_instagram_stories_total = spark.sql(f"""
                                SELECT
                                    CreatedDate,
                                    StoryID,
                                    MediaType,
                                    Username,
                                    Caption,
                                    TotalImpressions,
                                    TotalReplies,
                                    TotalShares,
                                    TemporaryEngagementRateInPercent,
                                    TotalSwipesForward,
                                    TotalTapBack,
                                    TotalTabExit,
                                    TotalTapForward,
                                    TotalReach
                                FROM datif_pz_uk_{env}.03_transformed.instagram_organic_stories_total
                              """)

fn_overwrite_table(df_source=df_instagram_stories_total, target_schema_name=pz_target_schema_name, target_table_name="instagram_organic_stories_total", target_path=target_path)                              

# COMMAND ----------

# MAGIC %md
# MAGIC ## X Organic Post

# COMMAND ----------

# load data from PZ-03_transformed into dataframe
df_x_total = spark.sql(f"""
                            SELECT
                                PostID,
                                CreatedDate,
                                InReplyToStatusID,
                                PostMessage,
                                PostURL,
                                PostType,
                                TotalImpressions,
                                TotalLikes,
                                TotalReposts,
                                TotalReplies,
                                TotalEngagements,
                                TotalFollows,
                                TotalClicks,
                                TotalLinkClicks,
                                TotalAppClicks,
                                TotalCardEngagements,
                                EngagementRateInPercent,
                                CLevelErwaehnungen,
                                TRIM(Themenbereich1) AS SubjectArea1,
                                TRIM(Themenbereich2) AS SubjectArea2,
                                TRIM(Themenbereich3) AS SubjectArea3,
                                Themenbereich1_Conf AS SubjectArea1_Confidence,
                                Themenbereich2_Conf AS SubjectArea2_Confidence,
                                Themenbereich3_Conf AS SubjectArea3_Confidence,
                                -- IF(Strategie2030 > {strategic_areas_threshold}, TRUE, FALSE) AS Strategie2030,
                                -- IF(FinanzierungEnergiewende > {strategic_areas_threshold}, TRUE, FALSE) AS FinanzierungEnergiewende,
                                -- IF(EMobilitaet > {strategic_areas_threshold}, TRUE, FALSE) AS EMobilitaet,
                                -- IF(Performancekultur > {strategic_areas_threshold}, TRUE, FALSE) AS Performancekultur,
                                -- IF(VernetzeEnergiewelt > {strategic_areas_threshold}, TRUE, FALSE) AS VernetzeEnergiewelt,
                                -- -- IF(Commodity > {strategic_areas_threshold}, TRUE, FALSE) AS Commodity,
                                -- IF(TransformationGasnetzeWasserstoff > {strategic_areas_threshold}, TRUE, FALSE) AS TransformationGasnetzeWasserstoff,
                                -- IF(ErneuerbareEnergien > {strategic_areas_threshold}, TRUE, FALSE) AS ErneuerbareEnergien,
                                -- IF(DisponibleErzeugung > {strategic_areas_threshold}, TRUE, FALSE) AS DisponibleErzeugung,
                                -- IF(IntelligenteStromnetze > {strategic_areas_threshold}, TRUE, FALSE) AS IntelligenteStromnetze,
                                -- IF(EnBWAlsArbeitgeberIn > {strategic_areas_threshold}, TRUE, FALSE) AS EnBWAlsArbeitgeberIn,
                                -- IF(NachhaltigkeitCSRESG > {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
                                -- IF(MarkeEnBW > {strategic_areas_threshold}, TRUE, FALSE) AS MarkeEnBW
                                Strategie2030,
                                FinanzierungEnergiewende,
                                EMobilitaet,
                                VernetzeEnergiewelt,
                                TransformationGasnetzeWasserstoff,
                                ErneuerbareEnergien,
                                DisponibleErzeugung,
                                IntelligenteStromnetze,
                                EnBWAlsArbeitgeberIn,
                                NachhaltigkeitCSRESG,
                                MarkeEnBW
                            FROM datif_pz_uk_{env}.03_transformed.x_organic_total
                            """)                      

# COMMAND ----------

# List of columns to check for true values
columns_to_check = [
    "Strategie2030", "FinanzierungEnergiewende", "EMobilitaet", 
    "VernetzeEnergiewelt", "TransformationGasnetzeWasserstoff", "ErneuerbareEnergien", 
    "DisponibleErzeugung", "IntelligenteStromnetze", "EnBWAlsArbeitgeberIn", 
    "NachhaltigkeitCSRESG", "MarkeEnBW"
]

# Create a new column 'Themen' with the column names where the value is >= 0.8
df_x_total = df_x_total.withColumn(
    "StrategischeThemen",
    concat_ws(";", *[when(col(c) >= 0.8, c) for c in columns_to_check])
)\
    .withColumn(
    "StrategischeThemen",
    when(trim(col("StrategischeThemen")) == "", "Kein strategisches Thema").otherwise(col("StrategischeThemen"))
)

df_x_total = df_x_total.select('PostID', 'CreatedDate', 'InReplyToStatusID', 'PostMessage', 'PostURL', 'PostType', 'TotalImpressions', 'TotalLikes', 'TotalReposts', 'TotalReplies', 'TotalEngagements', 'TotalFollows', 'TotalClicks','TotalLinkClicks', 'TotalAppClicks', 'TotalCardEngagements', 'CLevelErwaehnungen', 'SubjectArea1', 'SubjectArea2', 'SubjectArea3', 'SubjectArea1_Confidence', 'SubjectArea2_Confidence', 'SubjectArea3_Confidence', 'StrategischeThemen')


fn_overwrite_table(df_source=df_x_total, target_schema_name=pz_target_schema_name, target_table_name="x_organic_total", target_path=target_path)      

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linked Organic Post

# COMMAND ----------

# # load data from PZ-03_transformed into dataframe
# df_linkedin_post_total = spark.sql(f"""
#                                    SELECT 
#                                     PostID,
#                                     CreatedDate,
#                                     PostTitle,
#                                     PostContent,
#                                     PostURL,
#                                     ContentType,
#                                     TotalImpressions,
#                                     TotalLikes,
#                                     TotalShares,
#                                     TotalComments,
#                                     TotalClicks,
#                                     TotalCTR,
#                                     EngagementRateInPercent,
#                                     CLevelErwaehnungen,
#                                     TRIM(Themenbereich1) AS SubjectArea1,
#                                     TRIM(Themenbereich2) AS SubjectArea2,
#                                     TRIM(Themenbereich3) AS SubjectArea3,
#                                     Themenbereich1_Conf AS SubjectArea1_Confidence,
#                                     Themenbereich2_Conf AS SubjectArea2_Confidence,
#                                     Themenbereich3_Conf AS SubjectArea3_Confidence,
#                                     -- IF(Strategie2030 > {strategic_areas_threshold}, TRUE, FALSE) AS Strategie2030,
#                                     -- IF(FinanzierungEnergiewende > {strategic_areas_threshold}, TRUE, FALSE) AS FinanzierungEnergiewende,
#                                     -- IF(EMobilitaet > {strategic_areas_threshold}, TRUE, FALSE) AS EMobilitaet,
#                                     -- IF(Performancekultur > {strategic_areas_threshold}, TRUE, FALSE) AS Performancekultur,
#                                     -- IF(VernetzeEnergiewelt > {strategic_areas_threshold}, TRUE, FALSE) AS VernetzeEnergiewelt,
#                                     -- -- IF(Commodity > {strategic_areas_threshold}, TRUE, FALSE) AS Commodity,
#                                     -- IF(TransformationGasnetzeWasserstoff > {strategic_areas_threshold}, TRUE, FALSE) AS TransformationGasnetzeWasserstoff,
#                                     -- IF(ErneuerbareEnergien > {strategic_areas_threshold}, TRUE, FALSE) AS ErneuerbareEnergien,
#                                     -- IF(DisponibleErzeugung > {strategic_areas_threshold}, TRUE, FALSE) AS DisponibleErzeugung,
#                                     -- IF(IntelligenteStromnetze > {strategic_areas_threshold}, TRUE, FALSE) AS IntelligenteStromnetze,
#                                     -- IF(EnBWAlsArbeitgeberIn > {strategic_areas_threshold}, TRUE, FALSE) AS EnBWAlsArbeitgeberIn,
#                                     -- IF(NachhaltigkeitCSRESG > {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
#                                     -- IF(MarkeEnBW > {strategic_areas_threshold}, TRUE, FALSE) AS MarkeEnBW
#                                     Strategie2030,
#                                     FinanzierungEnergiewende,
#                                     EMobilitaet,
#                                     Performancekultur,
#                                     VernetzeEnergiewelt,
#                                     TransformationGasnetzeWasserstoff,
#                                     ErneuerbareEnergien,
#                                     DisponibleErzeugung,
#                                     IntelligenteStromnetze,
#                                     EnBWAlsArbeitgeberIn,
#                                     NachhaltigkeitCSRESG,
#                                     MarkeEnBW
#                                 FROM 03_transformed.linkedin_organic_total
#                                 """)                                

# COMMAND ----------

# # List of columns to check for true values
# columns_to_check = [
#     "Strategie2030", "FinanzierungEnergiewende", "EMobilitaet", "Performancekultur", 
#     "VernetzeEnergiewelt", "TransformationGasnetzeWasserstoff", "ErneuerbareEnergien", 
#     "DisponibleErzeugung", "IntelligenteStromnetze", "EnBWAlsArbeitgeberIn", 
#     "NachhaltigkeitCSRESG", "MarkeEnBW"
# ]

# # Create a new column 'Themen' with the column names where the value is >= 0.8
# df_linkedin_post_total = df_linkedin_post_total.withColumn(
#     "StrategischeThemen",
#     concat_ws(";", *[when(col(c) >= 0.8, c) for c in columns_to_check])
# )\
#     .withColumn(
#     "StrategischeThemen",
#     when(trim(col("StrategischeThemen")) == "", "Kein strategisches Thema").otherwise(col("StrategischeThemen"))
# )

# df_linkedin_post_total = df_linkedin_post_total.select('PostID', 'CreatedDate','PostTitle','PostContent','PostURL','ContentType','TotalImpressions','TotalLikes','TotalShares','TotalComments','TotalClicks','TotalCTR','CLevelErwaehnungen', 'SubjectArea1', 'SubjectArea2', 'SubjectArea3', 'SubjectArea1_Confidence', 'SubjectArea2_Confidence', 'SubjectArea3_Confidence', 'StrategischeThemen')


# fn_overwrite_table(df_source=df_linkedin_post_total, target_schema_name=pz_target_schema_name, target_table_name="linkedin_organic_post_total", target_path=target_path)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## LinkedIn Organic Video

# COMMAND ----------

# # load data from PZ-03_transformed into dataframe
# df_linkedin_video_total = spark.sql(f"""
#                                    SELECT 
#                                     VideoID,
#                                     CreatedDate,
#                                     FirstPublishTime,
#                                     VideoText,
#                                     PostURL,
#                                     ContentType,
#                                     Author,
#                                     Origin,
#                                     Visibility,
#                                     TotalImpressions,
#                                     TotalLikes,
#                                     TotalShares,
#                                     TotalComments,
#                                     TotalViews,
#                                     TotalClicks,
#                                     TotalViewers,
#                                     TotalTimeWatchedForVideoViews,
#                                     TotalTimeWatched,
#                                     EngagementRateInPercent,
#                                     CLevelErwaehnungen,
#                                     TRIM(Themenbereich1) AS SubjectArea1,
#                                     TRIM(Themenbereich2) AS SubjectArea2,
#                                     TRIM(Themenbereich3) AS SubjectArea3,
#                                     Themenbereich1_Conf AS SubjectArea1_Confidence,
#                                     Themenbereich2_Conf AS SubjectArea2_Confidence,
#                                     Themenbereich3_Conf AS SubjectArea3_Confidence,
#                                     -- IF(Strategie2030 > {strategic_areas_threshold}, TRUE, FALSE) AS Strategie2030,
#                                     -- IF(FinanzierungEnergiewende > {strategic_areas_threshold}, TRUE, FALSE) AS FinanzierungEnergiewende,
#                                     -- IF(EMobilitaet > {strategic_areas_threshold}, TRUE, FALSE) AS EMobilitaet,
#                                     -- IF(Performancekultur > {strategic_areas_threshold}, TRUE, FALSE) AS Performancekultur,
#                                     -- IF(VernetzeEnergiewelt > {strategic_areas_threshold}, TRUE, FALSE) AS VernetzeEnergiewelt,
#                                     -- -- IF(Commodity > {strategic_areas_threshold}, TRUE, FALSE) AS Commodity,
#                                     -- IF(TransformationGasnetzeWasserstoff > {strategic_areas_threshold}, TRUE, FALSE) AS TransformationGasnetzeWasserstoff,
#                                     -- IF(ErneuerbareEnergien > {strategic_areas_threshold}, TRUE, FALSE) AS ErneuerbareEnergien,
#                                     -- IF(DisponibleErzeugung > {strategic_areas_threshold}, TRUE, FALSE) AS DisponibleErzeugung,
#                                     -- IF(IntelligenteStromnetze > {strategic_areas_threshold}, TRUE, FALSE) AS IntelligenteStromnetze,
#                                     -- IF(EnBWAlsArbeitgeberIn > {strategic_areas_threshold}, TRUE, FALSE) AS EnBWAlsArbeitgeberIn,
#                                     -- IF(NachhaltigkeitCSRESG > {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
#                                     -- IF(MarkeEnBW > {strategic_areas_threshold}, TRUE, FALSE) AS MarkeEnBW
#                                     Strategie2030,
#                                     FinanzierungEnergiewende,
#                                     EMobilitaet,
#                                     Performancekultur,
#                                     VernetzeEnergiewelt,
#                                     TransformationGasnetzeWasserstoff,
#                                     ErneuerbareEnergien,
#                                     DisponibleErzeugung,
#                                     IntelligenteStromnetze,
#                                     EnBWAlsArbeitgeberIn,
#                                     NachhaltigkeitCSRESG,
#                                     MarkeEnBW
#                                 FROM 03_transformed.linkedin_organic_video_total
#                                 """)
                             

# COMMAND ----------

# # List of columns to check for true values
# columns_to_check = [
#     "Strategie2030", "FinanzierungEnergiewende", "EMobilitaet", "Performancekultur", 
#     "VernetzeEnergiewelt", "TransformationGasnetzeWasserstoff", "ErneuerbareEnergien", 
#     "DisponibleErzeugung", "IntelligenteStromnetze", "EnBWAlsArbeitgeberIn", 
#     "NachhaltigkeitCSRESG", "MarkeEnBW"
# ]

# # Create a new column 'Themen' with the column names where the value is >= 0.8
# df_linkedin_video_total = df_linkedin_video_total.withColumn(
#     "StrategischeThemen",
#     concat_ws(";", *[when(col(c) >= 0.8, c) for c in columns_to_check])
# )\
#     .withColumn(
#     "StrategischeThemen",
#     when(trim(col("StrategischeThemen")) == "", "Kein strategisches Thema").otherwise(col("StrategischeThemen"))
# )

# df_linkedin_video_total = df_linkedin_video_total.select('VideoID','CreatedDate','FirstPublishTime','VideoText','PostURL','ContentType','Author','Origin','Visibility','TotalImpressions','TotalLikes','TotalShares','TotalComments',
# 'TotalViews','TotalClicks','TotalViewers','TotalTimeWatchedForVideoViews','CLevelErwaehnungen', 'SubjectArea1', 'SubjectArea2', 'SubjectArea3', 'SubjectArea1_Confidence', 'SubjectArea2_Confidence', 'SubjectArea3_Confidence', 'StrategischeThemen')


# fn_overwrite_table(df_source=df_linkedin_video_total, target_schema_name=pz_target_schema_name, target_table_name="linkedin_organic_video_total", target_path=target_path)   

# COMMAND ----------

# MAGIC %md
# MAGIC ## LinkedIn Organic Consolidated

# COMMAND ----------

df_linkedin_total = spark.sql(f"""
                            SELECT 
                            PostID,
                            CreatedDate,
                            PostURL,
                            PostContent,
                            ContentType,
                            TotalImpressions,
                            TotalLikes,
                            TotalShares,
                            TotalComments,
                            TotalClicks,
                            TotalViews,
                            (TotalLikes + TotalShares + TotalComments + TotalClicks) AS Engagement,
                            EngagementRateInPercent,
                            TRIM(Themenbereich1) AS SubjectArea1,
                            TRIM(Themenbereich2) AS SubjectArea2,
                            TRIM(Themenbereich3) AS SubjectArea3,
                            Themenbereich1_Conf AS SubjectArea1_Confidence,
                            Themenbereich2_Conf AS SubjectArea2_Confidence,
                            Themenbereich3_Conf AS SubjectArea3_Confidence,
                            Strategie2030,
                            FinanzierungEnergiewende,
                            EMobilitaet,
                            VernetzeEnergiewelt,
                            TransformationGasnetzeWasserstoff,
                            ErneuerbareEnergien,
                            DisponibleErzeugung,
                            IntelligenteStromnetze,
                            EnBWAlsArbeitgeberIn,
                            NachhaltigkeitCSRESG,
                            MarkeEnBW
                        FROM datif_pz_uk_{env}.03_transformed.linkedin_organic_total
                              """)

# COMMAND ----------

# List of columns to check for true values
columns_to_check = [
    "Strategie2030", "FinanzierungEnergiewende", "EMobilitaet", 
    "VernetzeEnergiewelt", "TransformationGasnetzeWasserstoff", "ErneuerbareEnergien", 
    "DisponibleErzeugung", "IntelligenteStromnetze", "EnBWAlsArbeitgeberIn", 
    "NachhaltigkeitCSRESG", "MarkeEnBW"
]

# Create a new column 'Themen' with the column names where the value is >= 0.8
df_linkedin_total = df_linkedin_total.withColumn(
    "StrategischeThemen",
    concat_ws(";", *[when(col(c) >= 0.8, c) for c in columns_to_check])
)\
    .withColumn(
    "StrategischeThemen",
    when(trim(col("StrategischeThemen")) == "", "Kein strategisches Thema").otherwise(col("StrategischeThemen"))
)

df_linkedin_total = df_linkedin_total.select('PostID','CreatedDate','PostURL','PostContent','ContentType', 'TotalImpressions','TotalLikes','TotalShares','TotalComments','TotalClicks', "TotalViews", 'Engagement', 'EngagementRateInPercent','SubjectArea1', 'SubjectArea2', 'SubjectArea3', 'SubjectArea1_Confidence', 'SubjectArea2_Confidence', 'SubjectArea3_Confidence', 'StrategischeThemen')


fn_overwrite_table(df_source=df_linkedin_total, target_schema_name=pz_target_schema_name, target_table_name="linkedin_organic_total", target_path=target_path)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Youtube Organic Post

# COMMAND ----------

# load data from PZ-03_transformed into dataframe
df_youtube_total = spark.sql(f"""
                                SELECT
                                 CreatedDate,
                                 VideoID,
                                 ChannelID,
                                 VideoTitle,
                                 VideoURL,
                                 Tags,
                                 VideoDescription,
                                --  Duration,
                                 TotalViews,
                                 TotalLikes,
                                 TotalDislikes,
                                 TotalComments,
                                 TotalShares,
                                 EngagementRateInPercent,
                                 TotalSubscribersGained,
                                 TotalSubscribersLost,
                                 TotalCardImpressions,
                                 TotalCardTeaserImpressions,
                                 TotalAnnotationClickableImpressions,
                                 TotalAnnotationClosableImpressions,
                                --  TotalEstimatedMinutesWatchedPremium,
                                 TotalEstimatedMinutesWatched,
                                --  TotalVideosAddedToPlaylists,
                                --  TotalVideosRemovedFromPlaylists,
                                 TotalViewsPremium,
                                 TotalAnnotationClicks,
                                 TotalAverageViewDuration,
                                 TRIM(Themenbereich1) AS SubjectArea1,
                                 TRIM(Themenbereich2) AS SubjectArea2,
                                 TRIM(Themenbereich3) AS SubjectArea3,
                                 Themenbereich1_Conf AS SubjectArea1_Confidence,
                                 Themenbereich2_Conf AS SubjectArea2_Confidence,
                                 Themenbereich3_Conf AS SubjectArea3_Confidence,
                                 Strategie2030,
                                 FinanzierungEnergiewende,
                                 EMobilitaet,
                                 -- Performancekultur,
                                 VernetzeEnergiewelt,
                                --  Commodity Commodity,
                                 TransformationGasnetzeWasserstoff,
                                 ErneuerbareEnergien,
                                 DisponibleErzeugung,
                                 IntelligenteStromnetze,
                                 EnBWAlsArbeitgeberIn,
                                 NachhaltigkeitCSRESG,
                                 MarkeEnBW,
                                 Owner
                                FROM datif_pz_uk_{env}.03_transformed.youtube_organic_post_total
                                """)

# COMMAND ----------

from pyspark.sql.functions import expr, array, when, col, concat_ws, trim, regexp_replace

# List of columns to check for true values
columns_to_check = [
    "Strategie2030", "FinanzierungEnergiewende", "EMobilitaet", 
    "VernetzeEnergiewelt", "TransformationGasnetzeWasserstoff", "ErneuerbareEnergien", 
    "DisponibleErzeugung", "IntelligenteStromnetze", "EnBWAlsArbeitgeberIn", 
    "NachhaltigkeitCSRESG", "MarkeEnBW"
]

# Create a new column 'Themen' with the column names where the value is >= 0.8
df_youtube_total = df_youtube_total.withColumn(
    "StrategischeThemen",
    concat_ws(";", *[when(col(c) >= 0.8, c) for c in columns_to_check])
)\
    .withColumn(
    "StrategischeThemen",
    when(trim(col("StrategischeThemen")) == "", "Kein strategisches Thema").otherwise(col("StrategischeThemen"))
)
    
df_youtube_total = df_youtube_total.select('VideoID','CreatedDate','ChannelID','VideoTitle', 'VideoURL', 'Tags','TotalViews','TotalLikes','TotalDislikes','TotalComments','TotalShares','EngagementRateInPercent','TotalSubscribersGained','TotalSubscribersLost','TotalCardImpressions','TotalCardTeaserImpressions','TotalAnnotationClickableImpressions','TotalAnnotationClosableImpressions','TotalEstimatedMinutesWatched','TotalViewsPremium','TotalAnnotationClicks','TotalAverageViewDuration', 'SubjectArea1', 'SubjectArea2', 'SubjectArea3', 'SubjectArea1_Confidence', 'SubjectArea2_Confidence', 'SubjectArea3_Confidence', 'StrategischeThemen', 'Owner')

fn_overwrite_table(df_source=df_youtube_total, target_schema_name=pz_target_schema_name, target_table_name="youtube_organic_post_total", target_path=target_path)

