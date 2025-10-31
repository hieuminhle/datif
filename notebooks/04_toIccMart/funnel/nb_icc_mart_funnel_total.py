# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung Social Media Daten via Funnel - Total Aggregation
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der FUnnel API abgezogenen Daten der Social Media Daten wurden in der DZ unter dem Schema 02_cleaned für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Total Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggerde wird das notebook über die pipeline '00-1000-Funnel-orchestrate'.
# MAGIC
# MAGIC ---
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 03_transformed.facebook_organic_total
# MAGIC - 03_transformed.instagram_organic_total
# MAGIC - 03_transformed.x_organic_total
# MAGIC - 03_transformed.linkedin_organic_post_total
# MAGIC - 03_transformed.linkedin_organic_video_total
# MAGIC
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - 04_icc_mart.facebook_organic_total
# MAGIC - 04_icc_mart.instagram_organic_total
# MAGIC - 04_icc_mart.x_organic_total
# MAGIC - 04_icc_mart.linkedin_organic_post_total
# MAGIC - 04_icc_mart.linkedin_organic_video_total
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 13.02.2025 Max Mustermann: Schema Updates
# MAGIC - 06.02.2025 Max Mustermann: Unity Catalog als Variable hinzugefügt
# MAGIC - 06.12.2024 Max Mustermann: C-Level-Tagging hinzugefügt
# MAGIC - 03.12.2024 Max Mustermann: Schemabereinigungen     
# MAGIC - 29.11.2024 Max Mustermann: Refactoring
# MAGIC - 13.11.2024 Max Mustermann: Strategische Themen hinzugefügt
# MAGIC - 03.11.2024 Max Mustermann: Init

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Parameters

# COMMAND ----------

pz_target_schema_name = "04_icc_mart"
target_path = "funnel"

#strategic_areas_threshold = 0.4
strategic_areas_threshold = 0.8

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04-icc-mart

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facebook Organic Posts

# COMMAND ----------

# load data from PZ-03_transformed into dataframe
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
                                    -- IF(AAlsArbeitgeberIn >= {strategic_areas_threshold}, TRUE, FALSE) AS AAlsArbeitgeberIn,
                                    -- IF(NachhaltigkeitCSRESG >= {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
                                    -- IF(MarkeA >= {strategic_areas_threshold}, TRUE, FALSE) AS MarkeA
                                    CASE 
                                        WHEN GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) >= 0.8 THEN 
                                            CASE 
                                                WHEN Strategie2030 >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Strategie2030'
                                                WHEN FinanzierungEnergiewende >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'FinanzierungEnergiewende'
                                                WHEN EMobilitaet >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'EMobilitaet'
                                                WHEN Performancekultur >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Performancekultur'
                                                WHEN VernetzeEnergiewelt >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'VernetzeEnergiewelt'
                                                WHEN TransformationGasnetzeWasserstoff >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'TransformationGasnetzeWasserstoff'
                                                WHEN ErneuerbareEnergien >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'ErneuerbareEnergien'
                                                WHEN DisponibleErzeugung >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'DisponibleErzeugung'
                                                WHEN IntelligenteStromnetze >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'IntelligenteStromnetze'
                                                WHEN AAlsArbeitgeberIn >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'AAlsArbeitgeberIn'
                                                WHEN NachhaltigkeitCSRESG >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'NachhaltigkeitCSRESG'
                                                WHEN MarkeA >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'MarkeA'
                                            END
                                        ELSE 'Kein strategisches Thema'
                                    END AS StrategischesThema
                                FROM 03_transformed.facebook_organic_total
                                """)


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
                                    TotalImpressions,
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
                                    -- IF(AAlsArbeitgeberIn > {strategic_areas_threshold}, TRUE, FALSE) AS AAlsArbeitgeberIn,
                                    -- IF(NachhaltigkeitCSRESG > {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
                                    -- IF(MarkeA > {strategic_areas_threshold}, TRUE, FALSE) AS MarkeA
                                    CASE 
                                        WHEN GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) >= 0.8 THEN 
                                            CASE 
                                                WHEN Strategie2030 >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Strategie2030'
                                                WHEN FinanzierungEnergiewende >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'FinanzierungEnergiewende'
                                                WHEN EMobilitaet >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'EMobilitaet'
                                                WHEN Performancekultur >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Performancekultur'
                                                WHEN VernetzeEnergiewelt >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'VernetzeEnergiewelt'
                                                WHEN TransformationGasnetzeWasserstoff >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'TransformationGasnetzeWasserstoff'
                                                WHEN ErneuerbareEnergien >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'ErneuerbareEnergien'
                                                WHEN DisponibleErzeugung >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'DisponibleErzeugung'
                                                WHEN IntelligenteStromnetze >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'IntelligenteStromnetze'
                                                WHEN AAlsArbeitgeberIn >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'AAlsArbeitgeberIn'
                                                WHEN NachhaltigkeitCSRESG >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'NachhaltigkeitCSRESG'
                                                WHEN MarkeA >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'MarkeA'
                                            END
                                        ELSE 'Kein strategisches Thema'
                                    END AS StrategischesThema
                                FROM 03_transformed.instagram_organic_total
                                """)

fn_overwrite_table(df_source=df_instagram_total, target_schema_name=pz_target_schema_name, target_table_name="instagram_organic_total", target_path=target_path)                             

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
                                -- IF(AAlsArbeitgeberIn > {strategic_areas_threshold}, TRUE, FALSE) AS AAlsArbeitgeberIn,
                                -- IF(NachhaltigkeitCSRESG > {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
                                -- IF(MarkeA > {strategic_areas_threshold}, TRUE, FALSE) AS MarkeA
                                CASE 
                                    WHEN GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) >= 0.8 THEN 
                                        CASE 
                                            WHEN Strategie2030 >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Strategie2030'
                                            WHEN FinanzierungEnergiewende >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'FinanzierungEnergiewende'
                                            WHEN EMobilitaet >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'EMobilitaet'
                                            WHEN Performancekultur >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Performancekultur'
                                            WHEN VernetzeEnergiewelt >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'VernetzeEnergiewelt'
                                            WHEN TransformationGasnetzeWasserstoff >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'TransformationGasnetzeWasserstoff'
                                            WHEN ErneuerbareEnergien >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'ErneuerbareEnergien'
                                            WHEN DisponibleErzeugung >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'DisponibleErzeugung'
                                            WHEN IntelligenteStromnetze >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'IntelligenteStromnetze'
                                            WHEN AAlsArbeitgeberIn >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'AAlsArbeitgeberIn'
                                            WHEN NachhaltigkeitCSRESG >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'NachhaltigkeitCSRESG'
                                            WHEN MarkeA >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'MarkeA'
                                        END
                                    ELSE 'Kein strategisches Thema'
                                END AS StrategischesThema
                            FROM 03_transformed.x_organic_total
                            """)

fn_overwrite_table(df_source=df_x_total, target_schema_name=pz_target_schema_name, target_table_name="x_organic_total", target_path=target_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linked Organic Post

# COMMAND ----------

# load data from PZ-03_transformed into dataframe
df_linkedin_post_total = spark.sql(f"""
                                   SELECT 
                                    PostID,
                                    CreatedDate,
                                    PostTitle,
                                    PostContent,
                                    PostURL,
                                    ContentType,
                                    TotalImpressions,
                                    TotalLikes,
                                    TotalShares,
                                    TotalComments,
                                    TotalClicks,
                                    TotalCTR,
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
                                    -- IF(AAlsArbeitgeberIn > {strategic_areas_threshold}, TRUE, FALSE) AS AAlsArbeitgeberIn,
                                    -- IF(NachhaltigkeitCSRESG > {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
                                    -- IF(MarkeA > {strategic_areas_threshold}, TRUE, FALSE) AS MarkeA
                                    CASE 
                                        WHEN GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) >= 0.8 THEN 
                                            CASE 
                                                WHEN Strategie2030 >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Strategie2030'
                                                WHEN FinanzierungEnergiewende >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'FinanzierungEnergiewende'
                                                WHEN EMobilitaet >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'EMobilitaet'
                                                WHEN Performancekultur >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Performancekultur'
                                                WHEN VernetzeEnergiewelt >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'VernetzeEnergiewelt'
                                                WHEN TransformationGasnetzeWasserstoff >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'TransformationGasnetzeWasserstoff'
                                                WHEN ErneuerbareEnergien >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'ErneuerbareEnergien'
                                                WHEN DisponibleErzeugung >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'DisponibleErzeugung'
                                                WHEN IntelligenteStromnetze >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'IntelligenteStromnetze'
                                                WHEN AAlsArbeitgeberIn >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'AAlsArbeitgeberIn'
                                                WHEN NachhaltigkeitCSRESG >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'NachhaltigkeitCSRESG'
                                                WHEN MarkeA >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'MarkeA'
                                            END
                                        ELSE 'Kein strategisches Thema'
                                    END AS StrategischesThema
                                FROM 03_transformed.linkedin_organic_total
                                """)

fn_overwrite_table(df_source=df_linkedin_post_total, target_schema_name=pz_target_schema_name, target_table_name="linkedin_organic_total", target_path=target_path)                         

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linked Organic Video

# COMMAND ----------

# load data from PZ-03_transformed into dataframe
df_linkedin_video_total = spark.sql(f"""
                                   SELECT 
                                    VideoID,
                                    CreatedDate,
                                    FirstPublishTime,
                                    VideoText,
                                    PostURL,
                                    ContentType,
                                    Author,
                                    Origin,
                                    Visibility,
                                    TotalImpressions,
                                    TotalLikes,
                                    TotalShares,
                                    TotalComments,
                                    TotalViews,
                                    TotalClicks,
                                    TotalViewers,
                                    TotalTimeWatchedForVideoViews,
                                    TotalTimeWatched,
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
                                    -- IF(AAlsArbeitgeberIn > {strategic_areas_threshold}, TRUE, FALSE) AS AAlsArbeitgeberIn,
                                    -- IF(NachhaltigkeitCSRESG > {strategic_areas_threshold}, TRUE, FALSE) AS NachhaltigkeitCSRESG,
                                    -- IF(MarkeA > {strategic_areas_threshold}, TRUE, FALSE) AS MarkeA
                                    CASE 
                                        WHEN GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) >= 0.8 THEN 
                                            CASE 
                                                WHEN Strategie2030 >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Strategie2030'
                                                WHEN FinanzierungEnergiewende >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'FinanzierungEnergiewende'
                                                WHEN EMobilitaet >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'EMobilitaet'
                                                WHEN Performancekultur >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'Performancekultur'
                                                WHEN VernetzeEnergiewelt >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'VernetzeEnergiewelt'
                                                WHEN TransformationGasnetzeWasserstoff >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'TransformationGasnetzeWasserstoff'
                                                WHEN ErneuerbareEnergien >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'ErneuerbareEnergien'
                                                WHEN DisponibleErzeugung >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'DisponibleErzeugung'
                                                WHEN IntelligenteStromnetze >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'IntelligenteStromnetze'
                                                WHEN AAlsArbeitgeberIn >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'AAlsArbeitgeberIn'
                                                WHEN NachhaltigkeitCSRESG >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'NachhaltigkeitCSRESG'
                                                WHEN MarkeA >= GREATEST(Strategie2030, FinanzierungEnergiewende, EMobilitaet, Performancekultur, VernetzeEnergiewelt, TransformationGasnetzeWasserstoff, ErneuerbareEnergien, DisponibleErzeugung, IntelligenteStromnetze, AAlsArbeitgeberIn, NachhaltigkeitCSRESG, MarkeA) THEN 'MarkeA'
                                            END
                                        ELSE 'Kein strategisches Thema'
                                    END AS StrategischesThema
                                FROM 03_transformed.linkedin_organic_video_total
                                """)

fn_overwrite_table(df_source=df_linkedin_video_total, target_schema_name=pz_target_schema_name, target_table_name="linkedin_organic_video_total", target_path=target_path)                            

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
                                 Strategie2030,
                                 FinanzierungEnergiewende,
                                 EMobilitaet,
                                 Performancekultur,
                                 VernetzeEnergiewelt,
                                --  Commodity Commodity,
                                 TransformationGasnetzeWasserstoff,
                                 ErneuerbareEnergien,
                                 DisponibleErzeugung,
                                 IntelligenteStromnetze,
                                 AAlsArbeitgeberIn,
                                 NachhaltigkeitCSRESG,
                                 MarkeA
                                FROM 03_transformed.youtube_organic_post_total
                                """)

fn_overwrite_table(df_source=df_youtube_total, target_schema_name=pz_target_schema_name, target_table_name="youtube_organic_post_total", target_path=target_path)
