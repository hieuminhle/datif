# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Datenanbindung GA4 Daten via Funnel - Daily Export
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC - Die täglich von der Funnel API abgezogenen Daten der Social Media Daten wurden in der DZ unter dem Schema 02_cleaned für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook wird eine View der GA4 Daten angelegt, die nur auf die für UK relevanten URLs filtert.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_dz_{}.02_cleaned_uk_ga4.008_ga4_sessions_views_path_current_view
# MAGIC - datif_dz_{}.02_cleaned_uk_ga4.009_user_path_current_view
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_{}.03_transformed.ga4_sessions_current_view
# MAGIC - datif_pz_uk_{}.03_transformed.ga4_users_current_view
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC 16.07.2025 Justin Stange-Heiduk: Add datif_pz_uk_{env}.
# MAGIC - 21.02.2025 Svenja Schuder: Init

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

lst_url_link= [
    "www.enbw.com/unternehmen/themen/windkraft/zufallsstrom.html%",
    "www.enbw.com/unternehmen/themen/kohleausstieg/was-ist-energie.html%",
    "www.enbw.com/unternehmen/themen/digitalisierung/blockchain.html%",
    "www.enbw.com/unternehmen/themen/windkraft/sf6-in-windraedern.html%",
    "www.enbw.com/unternehmen/themen/netze/dunkelflaute.html%",
    "www.enbw.com/unternehmen/themen/klimaschutz/hochwasserschutz.html%",
    "www.enbw.com/unternehmen/themen/klimaschutz/energiewende-geht-voran.html%",
    "www.enbw.com/unternehmen/themen/windkraft/bedarfsgerechte-nachtkennzeichnung.html%",
    "www.enbw.com/unternehmen/themen/windkraft/vorteile-wind-und-solarenergie-fuer-gemeinden.html%",
    "www.enbw.com/unternehmen/themen/windkraft/windraeder-und-voegel.html%",
    "www.enbw.com/unternehmen/themen/netze/blackout.html%",
    "www.enbw.com/unternehmen/themen/netze/netzausbau.html%",
    "www.enbw.com/unternehmen/themen/netze/das-europaeische-stromnetz.html%",
    "www.enbw.com/unternehmen/themen/netze/gasnetz.html%",
    "www.enbw.com/unternehmen/themen/windkraft/wind-im-wald.html%",
    "www.enbw.com/unternehmen/themen/windkraft/windkraftanlagen.html%",
    "www.enbw.com/unternehmen/themen/wasserstoff/wasserstofftransport.html%",
    "www.enbw.com/unternehmen/themen/speicher/second-life-batterien.html%",
    "www.enbw.com/unternehmen/themen/speicher/batteriespeicher.html%",
    "www.enbw.com/unternehmen/themen/netze/redispatch.html%",
    "www.enbw.com/unternehmen/themen/wasserstoff/wasserstoffherstellung.html%",
    "www.enbw.com/unternehmen/themen/speicher/stromspeicher.html%",
    "www.enbw.com/unternehmen/themen/windkraft/loesungen-energiewende.html%",
    "www.enbw.com/unternehmen/themen/netze/digitalisierung-netzausbau.html%",
    "www.enbw.com/unternehmen/themen/netze/umspannwerke.html%",
    "www.enbw.com/unternehmen/themen/netze/stromnetze-so-kommt-der-strom-in-die-steckdose.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/die-wichtigsten-fragen-zu-lithium.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/lithium-umweltfreundlich-gewinnen.html%",
    "www.enbw.com/unternehmen/themen/windkraft/erneuerbare-energien.html%",
    "www.enbw.com/unternehmen/themen/kohleausstieg/kraftwerksstrategie.html%",
    "www.enbw.com/unternehmen/themen/wasserstoff/gruener-wasserstoff.html%",
    "www.enbw.com/unternehmen/themen/solarenergie/solarparks-fuer-die-energiewende.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/e-lkw-ladeinfrastruktur.html%",
    "www.enbw.com/unternehmen/themen/klimaschutz/oekologischer-fussabdruck.html%",
    "www.enbw.com/unternehmen/themen/wasserkraft/wasserkraftwerke.html%",
    "www.enbw.com/unternehmen/themen/klimaschutz/dekarbonisierung.html%",
    "www.enbw.com/unternehmen/themen/windkraft/offshore-technologie-aus-europa.html%",
    "www.enbw.com/unternehmen/themen/wasserstoff/wasserstoff-ist-ein-eckpfeiler-der-dekarbonisierung.html%",
    "www.enbw.com/unternehmen/themen/solarenergie/agri-photovoltaik-loest-gleich-zwei-probleme.html%",
    "www.enbw.com/unternehmen/themen/windkraft/das-beste-mittel-gegen-seekrankheit.html%",
    "www.enbw.com/unternehmen/themen/netze/netzentgelte-fuer-strom.html%",
    "www.enbw.com/unternehmen/themen/kohleausstieg/kraftwerke.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/barrierefreies-laden.html%",
    "www.enbw.com/unternehmen/themen/windkraft/onshore-wind-pfeiler-der-energiewende.html%",
    "www.enbw.com/unternehmen/themen/windkraft/windrad-transport.html%",
    "www.enbw.com/unternehmen/themen/windkraft/offshore-windparks-alle-fakten-zur-windenergie-auf-see.html%",
    "www.enbw.com/unternehmen/themen/waerme/gruene-waerme.html%",
    "www.enbw.com/unternehmen/themen/wasserstoff/wasserstoff-farben.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/e-bus-laden.html%",
    "www.enbw.com/unternehmen/themen/klimaschutz/fassadenwaerme.html%",
    "www.enbw.com/unternehmen/themen/klimaschutz/interview-jan-hegenberg.html%",
    "www.enbw.com/unternehmen/themen/wasserstoff/wasserstoffpaste.html%",
    "www.enbw.com/unternehmen/themen/geothermie/geothermie-projekt-daimler-truck.html%",
    "www.enbw.com/unternehmen/themen/digitalisierung/interview-frank-brech-cyber-security.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/automatisiertes-konduktives-laden-fuer-e-autos.html%",
    "www.enbw.com/unternehmen/themen/wasserstoff/wasserstoffmarkt.html%",
    "www.enbw.com/unternehmen/themen/digitalisierung/kritische-infrastruktur.html%",
    "www.enbw.com/unternehmen/themen/windkraft/windkraftanlagen-infraschall.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/ladesaeulen-3d-drucker.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/miniatur-wunderland-interview.html%",
    "www.enbw.com/unternehmen/themen/kohleausstieg/fakten-zum-fuel-switch.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/enbw-im-miniatur-wunderland.html%",
    "www.enbw.com/unternehmen/themen/windkraft/drohnen-in-der-energiewirtschaft.html%",
    "www.enbw.com/unternehmen/themen/windkraft/genehmigungsverfahren-windkraftanlage.html%",
    "www.enbw.com/unternehmen/themen/windkraft/warum-windraeder-stillstehen.html%",
    "www.enbw.com/unternehmen/themen/klimaschutz/flusswaermepumpe.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/e-mobilitaet-sind-unsere-netze-stabil-genug.html%",
    "www.enbw.com/unternehmen/themen/windkraft/windenergie-fachkraefte.html%",
    "www.enbw.com/unternehmen/themen/windkraft/windenergie-und-denkmalschutz.html%",
    "www.enbw.com/unternehmen/themen/netze/netzbooster-fuer-das-hoechstspannungsnetz.html%",
    "www.enbw.com/unternehmen/themen/wasserstoff/wasserstoff-aus-meerwasser.html%",
    "www.enbw.com/unternehmen/themen/klimaschutz/fernwaerme-interview-juedes.html% ",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/enbw-projekte-testen-neue-lademoeglichkeiten.html%",
    "www.enbw.com/unternehmen/themen/solarenergie/darum-sind-ppas-gut-fuer-die-energiewende.html%",
    "www.enbw.com/unternehmen/themen/solarenergie/photovoltaik-und-denkmalschutz.html%",
    "www.enbw.com/unternehmen/themen/waerme/gruene-fernwaerme.html%",
    "www.enbw.com/unternehmen/themen/netze/brownout.html%",
    "www.enbw.com/unternehmen/themen/klimaschutz/klimaneutralitaet.html%",
    "www.enbw.com/unternehmen/themen/solarenergie/integrierte-photovoltaik.html%",
    "www.enbw.com/unternehmen/themen/windkraft/bionik-und-windkraft.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/e-lkw-hohes-co2-einsparpotenzial.html%",
    "www.enbw.com/unternehmen/themen/elektromobilitaet/e-lkw-ladestationen.html",
    "www.enbw.com/unternehmen/themen/windkraft/kuenstliche-intelligenz-in-der-windkraft.html",
    "www.enbw.com/unternehmen/themen/windkraft/upcycling-alter-windkraftanlagen.html",
    "www.enbw.com/unternehmen/themen/netze/lichtverschmutzung-durch-strassenbeleuchtung.html",
    "www.enbw.com/unternehmen/themen/solarenergie/buergerbeteiligung-an-solarparks.html",
    "www.enbw.com/unternehmen/themen/digitalisierung/innovation-und-start-ups-in-der-energiewirtschaft.html",
    "www.enbw.com/unternehmen/themen/windkraft/windkraftanlagen-infraschall.html",
    "www.enbw.com/unternehmen/themen/klimaschutz/eu-taxonomie.html",
    "www.enbw.com/unternehmen/themen/windkraft/was-bringt-repowering.html",
    "www.enbw.com/unternehmen/themen/windkraft/wie-werden-windkraftanlagen-recycelt.html",
    "www.enbw.com/unternehmen/themen/klimaschutz/nachhaltige-investments-rettet-geld-die-welt.html",
    "www.enbw.com/unternehmen/themen/windkraft/windenergie-in-europa.html",
    "www.enbw.com/unternehmen/themen/windkraft/lebensraeume-offshore-windparks.html",
    "www.enbw.com/unternehmen/themen/windkraft/zehn-jahre-offshore-windkraft.html",
    "www.enbw.com/unternehmen/themen/solarenergie/solarparks-foerdern-artenvielfalt.html",
    "www.enbw.com/unternehmen/themen/windkraft/deutschlands-erste-flugwindkraftanlage.html",
    "www.enbw.com/unternehmen/themen/solarenergie/schwimmende-photovoltaikanlage.html",
    "www.enbw.com/unternehmen/themen/windkraft/virtuelles-kraftwerk.html"
    ]

# COMMAND ----------

conditions = ' OR '.join([f"Full_page_URL___GA4__Google_Analytics LIKE '{pattern}'" for pattern in lst_url_link])
print(conditions)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03-transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 - Sessions

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_sessions_current_view AS
            SELECT
                CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') as DATE) as Date,
                Session_campaign___GA4__Google_Analytics as Session_campaign,
                First_user_campaign___GA4__Google_Analytics as First_user_campaign,
                Page_path___GA4__Google_Analytics as Page_path,
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
                ROUND(Cast(Views___GA4__Google_Analytics / Sessions___GA4__Google_Analytics AS DOUBLE), 2) as Views_per_session
            FROM datif_dz_{env}.02_cleaned_uk_ga4.008_ga4_sessions_views_path_current_view
            WHERE Full_page_URL___GA4__Google_Analytics LIKE 'www.enbw.com/unternehmen/%' 
            OR Full_page_URL___GA4__Google_Analytics LIKE 'www.enbw.com/presse/%'
            OR Full_page_URL___GA4__Google_Analytics LIKE 'www.enbw.com/landingpages/%'
            ;
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 - Users

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_users_current_view AS
            SELECT
                CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') as DATE) as Date,
                Page_path___GA4__Google_Analytics as Page_path,
                Page_path__query_string___GA4__Google_Analytics as Page_path_query_string,
                Page_path__query_string_and_screen_class___GA4__Google_Analytics as Page_path_query_string_and_screen_class,
                Full_page_URL___GA4__Google_Analytics as Full_page_URL,
                Samples_read_rate___GA4__Google_Analytics as Samples_read_rate,
                Session_campaign___GA4__Google_Analytics as Session_campaign,
                Session_source__medium___GA4__Google_Analytics as Session_source__medium,
                CAST(Active_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_cam AS INTEGER) as Active_Users,
                CAST(Total_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_camp AS INTEGER) as Total_Users
            FROM datif_dz_{env}.02_cleaned_uk_ga4.009_ga4_user_path_current_view
            WHERE Full_page_URL___GA4__Google_Analytics LIKE 'www.enbw.com/unternehmen/%'
            OR Full_page_URL___GA4__Google_Analytics LIKE 'www.enbw.com/presse/%'
            OR Full_page_URL___GA4__Google_Analytics LIKE 'www.enbw.com/landingpages/%'
            ;
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GA4 - EcoJournal

# COMMAND ----------

# MAGIC %md
# MAGIC ### sessions daily

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_eco_journal_sessions_view AS
            SELECT
                CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') as DATE) as Date,
                Session_campaign___GA4__Google_Analytics as Session_campaign,
                First_user_campaign___GA4__Google_Analytics as First_user_campaign,
                Page_path___GA4__Google_Analytics as Page_path,
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
                ROUND(Cast(Views___GA4__Google_Analytics / Sessions___GA4__Google_Analytics AS DOUBLE), 2) as Views_per_session
            FROM datif_dz_{env}.02_cleaned_uk_ga4.008_ga4_sessions_views_path_current_view
            WHERE {conditions}
            ;
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### user daily

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE VIEW datif_pz_uk_{env}.03_transformed.ga4_eco_journal_users_view AS
            SELECT
                CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') as DATE) as Date,
                Page_path___GA4__Google_Analytics as Page_path,
                Page_path__query_string___GA4__Google_Analytics as Page_path_query_string,
                Page_path__query_string_and_screen_class___GA4__Google_Analytics as Page_path_query_string_and_screen_class,
                Full_page_URL___GA4__Google_Analytics as Full_page_URL,
                Samples_read_rate___GA4__Google_Analytics as Samples_read_rate,
                Session_campaign___GA4__Google_Analytics as Session_campaign,
                Session_source__medium___GA4__Google_Analytics as Session_source__medium,
                CAST(Active_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_cam AS INTEGER) as Active_Users,
                CAST(Total_Users___GA4__1_Day_Full_page_URL_Page_path_Page_path__query_string_Page_path__query_string_and_screen_class_Session_camp AS INTEGER) as Total_Users
            FROM datif_dz_{env}.02_cleaned_uk_ga4.009_ga4_user_path_current_view
            WHERE {conditions}
            ;
          """)

# COMMAND ----------


s = f"""
    SELECT 
        CAST(DATE_FORMAT(Date, 'yyyy-MM-dd') as DATE) as Date,
        Session_campaign___GA4__Google_Analytics as Session_campaign,
        First_user_campaign___GA4__Google_Analytics as First_user_campaign,
        Page_path___GA4__Google_Analytics as Page_path,
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
        ROUND(Cast(Views___GA4__Google_Analytics / Sessions___GA4__Google_Analytics AS DOUBLE), 2) as Views_per_session
    FROM datif_dz_{env}.02_cleaned_uk_ga4.008_ga4_sessions_views_path_current_view
    WHERE {conditions}
    """

query = spark.sql(s)
