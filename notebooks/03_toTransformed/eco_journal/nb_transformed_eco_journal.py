# Databricks notebook source
# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Transformationslogik
# MAGIC ## 03-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 Sessions x ECO Journal Daily
# MAGIC

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_eco_journal_sessions_daily_view AS
            SELECT
            CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') as DATE) as Date,
            Session_campaign___GA4__Google_Analytics as Session_campaign,
            First_user_campaign___GA4__Google_Analytics as First_user_campaign,
            Page_path___GA4__Google_Analytics as Page_path_GA4,
            Page_path__query_string___GA4__Google_Analytics as Page_path_query_string,
            Page_path__query_string_and_screen_class___GA4__Google_Analytics as Page_path_query_string_and_screen_class,
            Full_page_URL___GA4__Google_Analytics as Full_page_URL,
            Session_source__medium___GA4__Google_Analytics as Session_source__medium,
            CAST(Engaged_sessions___GA4__Google_Analytics AS INTEGER) as Engaged_sessions,
            CAST(Sessions___GA4__Google_Analytics AS INTEGER) as Sessions,
            CAST(Views___GA4__Google_Analytics AS INTEGER) as Views,
            CAST(User_engagement___GA4__Google_Analytics AS INTEGER) as User_engagement,
            CAST(Total_session_duration___GA4__Google_Analytics AS INTEGER) as Total_session_duration,
            ROUND(Cast((Sessions___GA4__Google_Analytics - Engaged_sessions___GA4__Google_Analytics) / Sessions___GA4__Google_Analytics AS DOUBLE)*100, 2) as Bounce_rate,
            ROUND(Cast(Total_session_duration___GA4__Google_Analytics / Sessions___GA4__Google_Analytics AS DOUBLE), 2) as Average_session_duration,
            ROUND(Cast(Views___GA4__Google_Analytics / Sessions___GA4__Google_Analytics AS DOUBLE), 2) as Views_per_session,
            eco_journal.Strategische_Themen as Strategische_Themen,
            eco_journal.Themenbereich1,
            eco_journal.Themenbereich2,
            eco_journal.Themenbereich3
            FROM datif_dz_{env}.`02_cleaned_uk_ga4`.`008_ga4_sessions_views_path_current_view` AS ga4_sessions
            INNER JOIN (
                SELECT
                  URL,
                  parse_url(URL, 'PATH') AS Page_Path_ECO,
                  Tag_1 as Themenbereich1,
                  Tag_2 as Themenbereich2,
                  Tag_3 as Themenbereich3,
                  Strategische_Themen
                FROM 02_cleaned.eco_journal_content_table
            ) AS eco_journal
            ON ga4_sessions.Page_path___GA4__Google_Analytics = eco_journal.Page_Path_ECO
            ORDER BY Date DESC
            ;
            """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 Users x ECO Journal Daily

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_eco_journal_users_daily_view AS
            SELECT
                CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') as DATE) as Date,
                Page_path___GA4__Google_Analytics as Page_path_GA4,
                Page_path__query_string___GA4__Google_Analytics as Page_path_query_string,
                Page_path__query_string_and_screen_class___GA4__Google_Analytics as Page_path_query_string_and_screen_class,
                Full_page_URL___GA4__Google_Analytics as Full_page_URL,
                Samples_read_rate___GA4__Google_Analytics as Samples_read_rate,
                Session_campaign___GA4__Google_Analytics as Session_campaign,
                Session_source__medium___GA4__Google_Analytics as Session_source__medium,
                CAST(Active_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_cam AS INTEGER) as Active_Users,
                CAST(Total_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_camp AS INTEGER) as Total_Users,
                eco_journal.Strategische_Themen as Strategische_Themen,
                eco_journal.Themenbereich1,
                eco_journal.Themenbereich2,
                eco_journal.Themenbereich3
                FROM datif_dz_{env}.`02_cleaned_uk_ga4`.`009_ga4_user_path_current_view` AS ga4_users
                INNER JOIN (
                    SELECT
                      URL,
                      parse_url(URL, 'PATH') AS Page_Path_ECO,
                      Tag_1 as Themenbereich1,
                      Tag_2 as Themenbereich2,
                      Tag_3 as Themenbereich3,
                      Strategische_Themen
                    FROM 02_cleaned.eco_journal_content_table
                ) AS eco_journal
                ON ga4_users.Page_path___GA4__Google_Analytics = eco_journal.Page_Path_ECO
            ;
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 Session Users x ECO Journal Daily 

# COMMAND ----------

test = spark.sql(f"""
CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_eco_journal_users_sessions_daily_view AS
SELECT

    -- Veröffentlichungs-Datum aus der Eco-Journal Content Tabelle
   MIN(CAST(DATE_FORMAT(ga4_sessions.Date, 'yyyy-MM-dd') AS DATE)) 
        OVER (PARTITION BY ga4_sessions.Page_path___GA4__Google_Analytics) AS Created_Date,
    -- CAST(DATE_FORMAT(eco_journal.Created_Date, 'yyyy-MM-dd') AS DATE) AS Neu_Veroeffentlichung, 

    CAST(DATE_FORMAT(ga4_sessions.Date, 'yyyy-MM-dd') AS DATE) AS Date, 
    
    -- Page Identifier
    ga4_sessions.Page_path___GA4__Google_Analytics as Page_path_GA4,

    -- URL-Komponenten
    FIRST(ga4_sessions.Page_path__query_string___GA4__Google_Analytics, TRUE) as Page_path_query_string,
    FIRST(ga4_sessions.Page_path__query_string_and_screen_class___GA4__Google_Analytics, TRUE) as Page_path_query_string_and_screen_class,
    FIRST(CONCAT('https://www.enbw.com',ga4_sessions.Page_path___GA4__Google_Analytics), TRUE) as Full_page_URL,

    -- Summierte Metriken
    SUM(CAST(ga4_sessions.Engaged_sessions___GA4__Google_Analytics AS INTEGER)) as Engaged_sessions,
    SUM(CAST(ga4_sessions.Sessions___GA4__Google_Analytics AS INTEGER)) as Sessions,
    SUM(CAST(ga4_sessions.Views___GA4__Google_Analytics AS INTEGER)) as Views,
    SUM(CAST(ga4_sessions.User_engagement___GA4__Google_Analytics AS INTEGER)) as User_engagement,
    SUM(CAST(ga4_sessions.Total_session_duration___GA4__Google_Analytics AS INTEGER)) as Total_session_duration,

    -- Durchschnittswerte
    ROUND(AVG(
        CAST((ga4_sessions.Sessions___GA4__Google_Analytics - ga4_sessions.Engaged_sessions___GA4__Google_Analytics) 
        / ga4_sessions.Sessions___GA4__Google_Analytics AS DOUBLE)
    ) * 100, 2) as Bounce_rate,

    ROUND(AVG(
        CAST(ga4_sessions.Total_session_duration___GA4__Google_Analytics 
        / ga4_sessions.Sessions___GA4__Google_Analytics AS DOUBLE)
    ), 2) as Average_session_duration,

    ROUND(
        SUM(ga4_sessions.Views___GA4__Google_Analytics) 
        / NULLIF(SUM(ga4_sessions.Sessions___GA4__Google_Analytics), 0), 2
    ) as Views_per_session,

        coalesce(ROUND(
        CAST(ANY_VALUE(user_agg.Active_Users) AS DOUBLE) / NULLIF(Views, 0),
        2), 0
    ) AS Active_Users_per_View,

    -- Userdaten aus 009er-View
    coalesce(ANY_VALUE(user_agg.Active_Users),0) AS Active_Users,
    coalesce(ANY_VALUE(user_agg.Total_Users),0) AS Total_Users,
    -- Inhalte aus Content-Tabelle
    FIRST(eco_journal.URL, TRUE) as URL,

    FIRST(eco_journal.Page_Path_ECO, TRUE) as Page_Path_ECO,
    FIRST(eco_journal.Strategische_Themen, TRUE) as Strategische_Themen,
    FIRST(eco_journal.Themenbereich1, TRUE) as Themenbereich1,
    FIRST(eco_journal.Themenbereich2, TRUE) as Themenbereich2,
    FIRST(eco_journal.Themenbereich3, TRUE) as Themenbereich3

FROM datif_dz_{env}.`02_cleaned_uk_ga4`.`008_ga4_sessions_views_path_current_view` AS ga4_sessions

INNER JOIN (
    SELECT
      URL,
      Veroeffentlicht as Created_Date,
      parse_url(URL, 'PATH') AS Page_Path_ECO,
      Strategische_Themen,
      Tag_1 as Themenbereich1,
      Tag_2 as Themenbereich2,
      Tag_3 as Themenbereich3
    FROM 02_cleaned.eco_journal_content_table
) AS eco_journal
ON ga4_sessions.Page_path___GA4__Google_Analytics = eco_journal.Page_Path_ECO

LEFT JOIN (
    SELECT
        CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') AS DATE) AS Date,
        Page_path___GA4__Google_Analytics AS Page_path_GA4,
        SUM(CAST(Active_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_cam AS INT)) AS Active_Users,
        SUM(CAST(Total_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_camp AS INT)) AS Total_Users
    FROM datif_dz_{env}.`02_cleaned_uk_ga4`.`009_ga4_user_path_current_view`
    GROUP BY 
        CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') AS DATE),
        Page_path___GA4__Google_Analytics
) AS user_agg
ON user_agg.Page_path_GA4 = ga4_sessions.Page_path___GA4__Google_Analytics
AND CAST(DATE_FORMAT(ga4_sessions.Date, 'yyyy-MM-dd') AS DATE) = user_agg.Date

GROUP BY 
    ga4_sessions.Page_path___GA4__Google_Analytics, 
    CAST(DATE_FORMAT(ga4_sessions.Date, 'yyyy-MM-dd') AS DATE), 
    CAST(DATE_FORMAT(eco_journal.Created_Date, 'yyyy-MM-dd') AS DATE)
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 Session x ECO Journal Total

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_eco_journal_sessions_total_view AS
SELECT
    -- Veröffentlichungs-Datum aus der Eco-Journal Content Tabelle
    FIRST(CAST(DATE_FORMAT(eco_journal.Created_Date, 'yyyy-MM-dd') AS DATE), TRUE) AS Created_Date,
    
    -- Page Identifier
    Page_path___GA4__Google_Analytics as Page_path_GA4,

    -- Erste bekannte Varianten von URL-Komponenten
    FIRST(Page_path__query_string___GA4__Google_Analytics, TRUE) as Page_path_query_string,
    FIRST(Page_path__query_string_and_screen_class___GA4__Google_Analytics, TRUE) as Page_path_query_string_and_screen_class,
    FIRST(Full_page_URL___GA4__Google_Analytics, TRUE) as Full_page_URL,

    -- Summierte Metriken
    SUM(CAST(Engaged_sessions___GA4__Google_Analytics AS INTEGER)) as Engaged_sessions,
    SUM(CAST(Sessions___GA4__Google_Analytics AS INTEGER)) as Sessions,
    SUM(CAST(Views___GA4__Google_Analytics AS INTEGER)) as Views,
    SUM(CAST(User_engagement___GA4__Google_Analytics AS INTEGER)) as User_engagement,
    SUM(CAST(Total_session_duration___GA4__Google_Analytics AS INTEGER)) as Total_session_duration,

    -- Durchschnittswerte (berechnet aus täglichen Aggregaten, kann ggf. angepasst werden)
    ROUND(AVG(
        CAST((Sessions___GA4__Google_Analytics - Engaged_sessions___GA4__Google_Analytics) / Sessions___GA4__Google_Analytics AS DOUBLE)
    ) * 100, 2) as Bounce_rate,

    ROUND(AVG(
        CAST(Total_session_duration___GA4__Google_Analytics / Sessions___GA4__Google_Analytics AS DOUBLE)
    ), 2) as Average_session_duration,

    ROUND(SUM(Views___GA4__Google_Analytics) / SUM(Sessions___GA4__Google_Analytics), 2) as Views_per_session,

    -- Inhalte aus Content-Tabelle (sollten eindeutig sein pro Page)
    FIRST(eco_journal.URL, TRUE) as URL,
    FIRST(eco_journal.Page_Path_ECO, TRUE) as Page_Path_ECO,
    FIRST(eco_journal.Themenbereich1, TRUE) as Themenbereich1,
    FIRST(eco_journal.Themenbereich2, TRUE) as Themenbereich2,
    FIRST(eco_journal.Themenbereich3, TRUE) as Themenbereich3

FROM datif_dz_{env}.`02_cleaned_uk_ga4`.`008_ga4_sessions_views_path_current_view` AS ga4_sessions

INNER JOIN (
    SELECT
      URL,
      Veroeffentlicht as Created_Date,
      parse_url(URL, 'PATH') AS Page_Path_ECO,
      Tag_1 as Themenbereich1,
      Tag_2 as Themenbereich2,
      Tag_3 as Themenbereich3
    FROM 02_cleaned.eco_journal_content_table
) AS eco_journal
ON ga4_sessions.Page_path___GA4__Google_Analytics = eco_journal.Page_Path_ECO

GROUP BY Page_path___GA4__Google_Analytics

""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 Users x ECO Journal Total 

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_eco_journal_users_total_view AS
SELECT
    -- Veröffentlichungs-Datum aus der Eco-Journal Content Tabelle
    FIRST(CAST(DATE_FORMAT(eco_journal.Created_Date, 'yyyy-MM-dd') AS DATE), TRUE) AS Created_Date,



    -- Gruppierungsschlüssel
    Page_path___GA4__Google_Analytics as Page_path_GA4,

    -- Erste auftretende URL-Varianten
    FIRST(Page_path__query_string___GA4__Google_Analytics, TRUE) as Page_path_query_string,
    FIRST(Page_path__query_string_and_screen_class___GA4__Google_Analytics, TRUE) as Page_path_query_string_and_screen_class,
    FIRST(Full_page_URL___GA4__Google_Analytics, TRUE) as Full_page_URL,

    -- Metriken aggregieren
    ROUND(AVG(Samples_read_rate___GA4__Google_Analytics), 2) as Samples_read_rate,
    SUM(CAST(Active_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_cam AS INTEGER)) as Active_Users,
    SUM(CAST(Total_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_camp AS INTEGER)) as Total_Users,

    -- Inhalte aus der eco_journal-Tabelle
    FIRST(eco_journal.URL, TRUE) as URL,
    FIRST(eco_journal.Page_Path_ECO, TRUE) as Page_Path_ECO,
    FIRST(eco_journal.Themenbereich1, TRUE) as Themenbereich1,
    FIRST(eco_journal.Themenbereich2, TRUE) as Themenbereich2,
    FIRST(eco_journal.Themenbereich3, TRUE) as Themenbereich3

FROM datif_dz_{env}.`02_cleaned_uk_ga4`.`009_ga4_user_path_current_view` AS ga4_users

INNER JOIN (
    SELECT
        URL,
        Veroeffentlicht as Created_Date,
        parse_url(URL, 'PATH') AS Page_Path_ECO,
        Tag_1 as Themenbereich1,
        Tag_2 as Themenbereich2,
        Tag_3 as Themenbereich3
    FROM 02_cleaned.eco_journal_content_table
) AS eco_journal
ON ga4_users.Page_path___GA4__Google_Analytics = eco_journal.Page_Path_ECO

GROUP BY ga4_users.Page_path___GA4__Google_Analytics

ORDER BY Created_Date ASC
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 Session Users x ECO Journal Total 

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_eco_journal_users_sessions_total_view AS

SELECT
    -- Veröffentlichungs-Datum aus der Eco-Journal Content Tabelle
    FIRST(CAST(DATE_FORMAT(eco_journal.Created_Date, 'yyyy-MM-dd') AS DATE), TRUE) AS Created_Date,

    -- Page Identifier
    eco_journal.Page_Path_ECO as Page_Path_GA4,

    -- Erste bekannte Varianten von URL-Komponenten
    FIRST(ga4_total.Page_path__query_string___GA4__Google_Analytics, TRUE) as Page_path_query_string,
    FIRST(ga4_total.Page_path__query_string_and_screen_class___GA4__Google_Analytics, TRUE) as Page_path_query_string_and_screen_class,
    FIRST(ga4_total.Full_page_URL___GA4__Google_Analytics, TRUE) as Full_page_URL,

    -- Summierte Metriken
    SUM(CAST(ga4_total.Engaged_sessions___GA4__Google_Analytics AS INTEGER)) as Engaged_sessions,
    SUM(CAST(ga4_total.Sessions___GA4__Google_Analytics AS INTEGER)) as Sessions,
    SUM(CAST(ga4_total.Views___GA4__Google_Analytics AS INTEGER)) as Views,
    SUM(CAST(ga4_total.User_engagement___GA4__Google_Analytics AS INTEGER)) as User_engagement,
    SUM(CAST(ga4_total.Total_session_duration___GA4__Google_Analytics AS INTEGER)) as Total_session_duration,

    -- Durchschnittswerte
    ROUND(AVG(
        CAST((ga4_total.Sessions___GA4__Google_Analytics - ga4_total.Engaged_sessions___GA4__Google_Analytics) / ga4_total.Sessions___GA4__Google_Analytics AS DOUBLE)
    ) * 100, 2) as Bounce_rate,

    ROUND(AVG(
        CAST(ga4_total.Total_session_duration___GA4__Google_Analytics / ga4_total.Sessions___GA4__Google_Analytics AS DOUBLE)
    ), 2) as Average_session_duration,

    ROUND(SUM(ga4_total.Views___GA4__Google_Analytics) / NULLIF(SUM(ga4_total.Sessions___GA4__Google_Analytics), 0), 2) as Views_per_session,

    ROUND(CAST(FIRST(ga4_users.Active_Users, TRUE) / NULLIF(SUM(ga4_total.Views___GA4__Google_Analytics), 0) AS DOUBLE), 2) as Active_Users_per_View,

    -- User-Metriken
    FIRST(ga4_users.Samples_read_rate, TRUE) as Samples_read_rate,
    FIRST(ga4_users.Active_Users, TRUE) as Active_Users,
    FIRST(ga4_users.Total_Users, TRUE) as Total_Users,

    -- Contentdaten aus eco_journal
    FIRST(eco_journal.URL, TRUE) as URL,
    FIRST(eco_journal.Page_Path_ECO, TRUE) as Page_Path_ECO,
    FIRST(eco_journal.Strategische_Themen, TRUE) as Strategische_Themen,
    FIRST(eco_journal.Themenbereich1, TRUE) as Themenbereich1,
    FIRST(eco_journal.Themenbereich2, TRUE) as Themenbereich2,
    FIRST(eco_journal.Themenbereich3, TRUE) as Themenbereich3,
    FIRST(eco_journal.Abstract, TRUE) as Abstract

FROM (
    SELECT
        URL,
        Veroeffentlicht as Created_Date,
        parse_url(URL, 'PATH') AS Page_Path_ECO,
        Strategische_Themen,
        Tag_1 as Themenbereich1,
        Tag_2 as Themenbereich2,
        Tag_3 as Themenbereich3,
        Abstract
    FROM 02_cleaned.eco_journal_content_table
) AS eco_journal

LEFT JOIN datif_dz_{env}.`02_cleaned_uk_ga4`.`008_ga4_sessions_views_path_current_view` AS ga4_total
    ON eco_journal.Page_Path_ECO = ga4_total.Page_path___GA4__Google_Analytics

LEFT JOIN (
    SELECT
        Page_path___GA4__Google_Analytics as Page_path_GA4,
        ROUND(AVG(Samples_read_rate___GA4__Google_Analytics), 2) as Samples_read_rate,
        SUM(CAST(Active_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_cam AS INTEGER)) as Active_Users,
        SUM(CAST(Total_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_camp AS INTEGER)) as Total_Users
    FROM datif_dz_{env}.`02_cleaned_uk_ga4`.`009_ga4_user_path_current_view`
    GROUP BY Page_path___GA4__Google_Analytics
) AS ga4_users
    ON eco_journal.Page_Path_ECO = ga4_users.Page_path_GA4

GROUP BY eco_journal.Page_Path_ECO

ORDER BY Created_Date ASC

""")

# COMMAND ----------

spark.catalog.refreshTable("03_transformed.ga4_eco_journal_users_sessions_total_view")
