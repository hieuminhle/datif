# Databricks notebook source
# MAGIC %md
# MAGIC # 03-Transformed: Datenbereitstellung Genios Medien Panel
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC Die täglich von der Genios API abgezogenen Daten wurden in der DZ unter dem Schema 02_cleaned_uk_genios für die PZ-UK bereitgestellt.
# MAGIC Mit diesem notebook werden die Tabellen täglich im 03-transformed schema der PZ updated.
# MAGIC Getriggerde wird das notebook über die pipeline 'tbd'.
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_dz_dev.02_cleaned_uk_genios.genios_enbw_online_media_panel_current_view
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_dev.03_transformed.genios_enbw_online_media_panel
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 27.01.2025 Sebastian Fastert: Init

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %run ../../common/nb_tagging_functions

# COMMAND ----------

from openai import AzureOpenAI
from typing import List, Dict
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import when, col, lit, concat, upper
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# DBTITLE 1,Parameters

target_schema_name = "03_transformed"
target_path = "genios"
limit_n = 1 # limit number of newly generated abstracts per load (the 'limit_n' number of newest published articles are used)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ### Online-Medien

# COMMAND ----------

df_argus_online = spark.read.table("datif_pz_uk_dev.03_transformed.argus_online_media_panel")

df_argus_online = df_argus_online.select(
    F.col("clipping_id"),
    F.col("clipping_headline"),
    F.col("clipping_publication_date"),
    F.col("clipping_abstract"),
    F.col("media_title"),
    F.col("media_editorial"),
    F.col("media_publisher"),
    F.col("media_gross_reach"),
    F.col("media_visits"),
    F.col("media_page_views"),
    F.col("media_unique_user"),
    F.col("media_sector"),
    F.col("media_state"),
    F.col("media_type")
)

publisher_title_editorial = {
    "ABZ Online": ["Patzer Verlag GmbH & Co. KG", "Allgemeine Bauzeitung", "Allgemeine Bauzeitung ABZ Redaktion"],
    "autobild.de":	["Axel Springer Auto Verlag GmbH", "Auto Bild",	"Auto Bild Redaktion"],
    "FAZ.NET": ["Frankfurter Allgemeine Zeitung GmbH", "FAZ.net", "FAZ.NET Redaktion"],
    "Heise online":	["Heise Medien GmbH & Co. KG", "heise online", "heise online Redaktion"],
    "Neue Presse Coburg online": ["Druck- und Verlagsanstalt Neue Presse GmbH", "Neue Presse Coburg", "Neue Presse Coburg Redaktion"],
    "Verlagshaus Jaumann online": ["Oberbadisches Verlagshaus Georg Jaumann GmbH&Co KG", "Die Oberbadische", "Die Oberbadische Redaktion"],
    "WELT ONLINE": ["Axel Springer SE",	"DIE WELT", "DIE WELT Redaktion"],
    "WirtschaftsWoche online": ["Handelsblatt GmbH", "WirtschaftsWoche Online", "Wirtschaftswoche Redaktion"]
}

def fill_genios_online(df):
    df = df.withColumn("media_publisher", lit(None))\
            .withColumn("media_title", lit(None))\
            .withColumn("media_editorial", lit(None))\
            .withColumn("media_gross_reach", lit(None))\
            .withColumn("media_visits", lit(None))\
            .withColumn("media_page_views", lit(None))\
            .withColumn("media_unique_user", lit(None))\
            .withColumn("media_sector", lit(None))\
            .withColumn("media_state", lit(None))\
            .withColumn("media_type", lit(None))

    for key, value in publisher_title_editorial.items():
        df = df.withColumn("media_publisher", 
                           when(col("Source") == key, lit(value[0]))
                           .otherwise(col("media_publisher")))
        df = df.withColumn("media_title", 
                           when(col("Source") == key, lit(value[1]))
                            .otherwise(col("media_title")))
        df = df.withColumn("media_editorial", 
                           when(col("Source") == key, lit(value[2]))
                           .otherwise(col("media_editorial")))
    
    return df

df_genios_online = spark.read.table("datif_pz_uk_dev.03_transformed.genios_enbw_online_media_panel")

df_genios_online = fill_genios_online(df_genios_online)

df_genios_online = df_genios_online.select(
    F.col("DocID").alias("clipping_id"),
    F.col("Title").alias("clipping_headline"),
    F.col("PublishingDay").alias("clipping_publication_date"),
    F.col("Abstract").alias("clipping_abstract"),
    F.col("media_title"),
    F.col("media_editorial"),
    F.col("media_publisher"),
    F.col("media_gross_reach"),
    F.col("media_visits"),
    F.col("media_page_views"),
    F.col("media_unique_user"),
    F.col("media_sector"),
    F.col("media_state"),
    F.col("media_type")
)

df_joined_online = df_genios_online.union(df_argus_online)
display(df_joined_online)

fn_overwrite_table(df_joined_online, target_schema_name=target_schema_name, target_table_name="genios_argus_online_media", target_path=target_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print-Medien

# COMMAND ----------

publisher_title_editorial = {
    "Acher und Bühler Bote": ["Badische Neueste Nachrichten Badendruck GmbH", "Acher- und Bühler Bote", "Acher- und Bühler Bote Redaktion"],
    "Badische Neueste Nachrichten": ["Badische Neueste Nachrichten Badendruck GmbH", "Badische Neueste Nachrichten", "Badische Neueste Nachrichten Redaktion"],
    "Badische Zeitung": ["Badischer Verlag GmbH & Co.KG", "Badische Zeitung", "Badische Zeitung Redaktion"],
    "Badisches Tagblatt": ["Badische Neueste Nachrichten Badendruck GmbH", "Badisches Tagblatt", "Badisches Tagblatt Redaktion"],
    "Beschaffung Aktuell": ["Konradin Medien GmbH", "Beschaffung Aktuell", "Beschaffung Aktuell Redaktion"],
    "Böblinger Bote": ["Wilhelm Schlecht GmbH & Co.KG", "Kreiszeitung Böblinger Bote", "Kreiszeitung Böblinger Bote Redaktion"],
    "Brettener Nachrichten": ["Badische Neueste Nachrichten Badendruck GmbH", "Badische Neueste Nachrichten", "Badische Neueste Nachrichten Redaktion"],
    "Bruchsaler Rundschau": ["Badische Neueste Nachrichten Badendruck GmbH", "Badische Neueste Nachrichten", "Badische Neueste Nachrichten Redaktion"],
    "Cannstatter Zeitung": ["Bechtle Verlag GmbH & Co. KG", "Cannstatter Zeitung Untertürkheimer Zeitung", "Cannstatter Zeitung Lokalredaktion"],
    "cannstatter-zeitung.de": ["Bechtle Verlag GmbH & Co. KG", "Cannstatter Zeitung Untertürkheimer Zeitung", "Cannstatter Zeitung Lokalredaktion"],
    "Die Oberbadische": ["Oberbadisches Verlagshaus Georg Jaumann GmbH&Co KG", "Die Oberbadische", "Die Oberbadische Redaktion"],
    "EID Energie Informationsdienst": ["Energiemarkt GmbH", "EiD Energie Informationsdienst", "Energie Informationsdienst Redaktion"],
    "Eßlinger Zeitung": ["Bechtle Verlag GmbH & Co. KG", "Eßlinger Zeitung", "Eßlinger Zeitung Redaktion"],
    "esslinger-zeitung.de": ["Bechtle Verlag GmbH & Co. KG", "Eßlinger Zeitung", "Eßlinger Zeitung Redaktion"],
    "F.A.Z. Frankfurter Allgemeine Zeitung (FAZ)": ["Frankfurter Allgemeine Zeitung GmbH", "Frankfurter Allgemeine Zeitung", "Frankfurter Allgemeine Redaktion"],
    "Frankfurter Rundschau": ["Frankfurter Rundschau GmbH", "Frankfurter Rundschau", "Frankfurter Rundschau Redaktion"],
    "Haller Tagblatt": ["Zeitungsverlag Schwäbisch Hall GmbH", "Haller Tagblatt", "Haller Tagblatt Redaktion"],
    "Handelsblatt": ["Handelsblatt GmbH", "Handelsblatt", "Handelsblatt Redaktion"],
    "Hohenloher Tagblatt": ["Südwest Presse Hohenlohe GmbH & Co KG", "Hohenloher Tagblatt",	"Hohenloher Tagblatt Redaktion"],
    "Kölnische Rundschau": ["M. DuMont Schauberg GmbH & Co.KG", "Kölnische Rundschau RKC Köln linksrheinisch", "Kölnische Rundschau Redaktion"],
    "Neckar-Chronik": ["Schwäbisches Tagblatt GmbH", "Südwest Presse Neckar Chronik", "Neckar Chronik Redaktion"],
    "Neue Presse (Coburg)": ["Druck- und Verlagsanstalt Neue Presse GmbH", "Neue Presse Coburg", "Neue Presse Coburg Redaktion"],
    "Neue Westfälische": ["Neue Westfälische GmbH & Co.KG", "Neue Westfälische Höxtersche Kreiszeitung", "Höxtersche Kreiszeitung Redaktion"],
    "Nordwest-Zeitung": ["Nordwest-Zeitung Verlagsgesellschaft mbH & Co. KG", "Nordwest Zeitung", "Nordwest-Zeitung Redaktion"],
    "Pforzheimer Kurier": ["Badische Neueste Nachrichten Badendruck GmbH", "Pforzheimer Kurier", "Pforzheimer Kurier Redaktion"],
    "Rheinische Post": ["Rheinische Post Verlagsgesellschaft mbH", "Rheinische Post", "Rheinische Post Redaktion"],
    "Rhein-Zeitung": ["Mittelrhein-Verlag GmbH", "Rhein-Zeitung", "Rhein-Zeitung Redaktion"],
    "Reutlinger General-Anzeiger": ["Reutlinger General-Anzeiger Zeitungsverlags GmbH & Co. KG", "Reutlinger General-Anzeiger", "Reutlinger General-Anzeiger Redaktion"],
    "Schwäbische Zeitung": ["Schwäbischer Verlag GmbH & Co. KG", "Schwäbische Zeitung", "Schwäbische Zeitung Redaktion"],
    "Schwäbisches Tagblatt": ["Schwäbisches Tagblatt GmbH", "Schwäbisches Tagblatt Tübinger Chronik", "Schwäbisches Tagblatt Redaktion"],
    "Schwarzwälder Bote": ["Schwarzwälder Bote Mediengesellschaft mbH", "Schwarzwälder Bote", "Schwarzwälder Bote Redaktion"],
    "schwarzwaelder-bote.de": ["Schwarzwälder Bote Mediengesellschaft mbH", "Schwarzwälder Bote", "Schwarzwälder Bote Redaktion"],
    "Sollinger Allgemeine": ["Verlag Dierichs GmbH & Co.KG", "HNA Sollinger Allgemeine", "Sollinger Allgemeine Redaktion"],
    "Stuttgarter Nachrichten": ["Stuttgarter Nachrichten Verlagsgesellschaft mbH", "Stuttgarter Nachrichten", "Stuttgarter Nachrichten Redaktion"],
    "Stuttgarter Zeitung": ["Stuttgarter Zeitung Verlagsgesellschaft mbH", "Stuttgarter Zeitung", "Stuttgarter Zeitung Redaktion"],
    "Südkurier": ["Südkurier GmbH Medienhaus", "Südkurier", "Südkurier Redaktion"],
    "ZfK-Zeitung für kommunale Wirtschaft": ["VKU Verlag GmbH",	"ZfK Zeitung für Kommunale Wirtschaft", "ZFK Zeitung für Kommunale Wirtschaft"]
}

titles = {
    "Acher- und Bühler Bote": ["Acher- und Bühler Bote", "Acher und Bühler Bote"],
    "Badische Neueste Nachrichten": ["Badische Neueste Nachrichten Ettlingen", "Badische Neueste Nachrichten Ettlingen", 
                                     "Badische Neueste Nachrichten Hardt", "Badische Neueste Nachrichten Karlsruhe", "Badische Neueste Nachrichten Mittelbaden",
                                     "Badische Neueste Nachrichten ST", "Badische Neueste Nachrichten Brettener Nachrichten", 
                                     "Badische Neueste Nachrichten Bruchsaler Rundschau"],
    "Badische Zeitung": ["Badische Zeitung Bad Säckingen", "Badische Zeitung Elztal", "Badische Zeitung Emmendingen Breisgau Kaiserstuhl", "Badische Zeitung Freiburg",
                         "Badische Zeitung Freiburg im Breisgau", "Badische Zeitung Hochschwarzwald", "Badische Zeitung Lörrach Weil am Rhein", 
                         "Badische Zeitung Region Freiburg"],
    "Badisches Tagblatt": ["Badische Neueste Nachrichten Badisches Tagblatt", "Badisches Tagblatt Murgtal", "Badisches Tagblatt Rastatt", "Badisches Tagblatt", "Badisches Tagblatt Rastatter Tageblatt"],
    #"Bild": ["Auto Bild", "Bild am Sonntag", "Bild Bremen", "Bild Bundesausgabe überregional", "Bild Stuttgart"],
    "Börsen-Zeitung": ["Börsen-Zeitung", "Börsen-Zeitung Spezial"],
    #"DIE WELT": ["DIE WELT", "Die Welt überregional", "WELT AM SONNTAG Frühausgabe Samstag Hamburg", "Welt am Sonntag Hamburg", "Welt am Sonntag überregional"],
    #"Der Neue Tag": ["Der Neue Tag Erbendorf Kemnath", "Der Neue Tag Eschenbach"],
    #"Der Sonntag": ["Der Sonntag Freiburg", "Der Sonntag im nördlichen Breisgau"],
    "EiD Energie Informationsdienst": ["EiD Energie Informationsdienst (E-Paper)", "EiD Energie Informationsdienst (Print)", "EID Energie Informationsdienst"],
    #"Euro": ["Euro", "Euro am Sonntag"],
    #"Focus": ["Focus", "Focus Money"],
    "Frankfurter Allgemeine Zeitung": ["Frankfurter Allgemeine Zeitung", "Frankfurter Allgemeine Sonntagszeitung"],
    "Frankfurter Rundschau": ["Frankfurter Rundschau Ausgabe D", "Frankfurter Rundschau Stadtausgabe (S)"],
    "Fränkische Nachrichten Buchen Walldürn": ["Fränkische Nachrichten Buchen Walldürn", "Fränkische Nachrichten Buchen/Walldürn"],
    "Heilbronner Stimme": ["Heilbronner Stimme", "Heilbronner Stimme Nord", "Heilbronner Stimme Stadtausgabe", "Heilbronner Stimme West"],
    "Hellweger Anzeiger": ["Hellweger Anzeiger Bergkamen", "Hellweger Anzeiger Unna"],
    "Kölnische Rundschau": ["Kölnische Rundschau", "Kölnische Rundschau RKC Köln linksrheinisch"],
    "Neue Presse Coburg": ["Neue Presse Coburg", "Neue Presse (Coburg)"],
    "Nordwest Zeitung": ["Nordwest Zeitung Der Münsterländer", "Nordwest Zeitung Oldenburger Nachrichten", "Nordwest-Zeitung"],
    "Ostsee-Zeitung Rostocker Zeitung": ["Ostsee-Zeitung - Rostocker Zeitung", "Ostsee-Zeitung Rostocker Zeitung"],
    "Rheinische Post": ["Rheinische Post D Düsseldorf", "Rheinische Post Erkelenzer Zeitung MG-ERK Erkelenz", "Rheinische Post MG/ML Mönchengladbach", 
                        "Rheinische Post Ne-GV Neuss-Grevenbroicher Zeitung"],
    "Rhein-Zeitung": ["Rhein-Zeitung Cochem-Cell Ausgabe D", "Rhein-Zeitung Koblenz"],
    "Rhein-Neckar-Zeitung Nordbadische Nachrichten": ["Rhein-Neckar-Zeitung - Nordbadische Nachrichten", "Rhein-Neckar-Zeitung Nordbadische Nachrichten"],
    "Schwäbische Zeitung": ["Schwäbische Zeitung Alb-Donau Laichingen", "Schwäbische Zeitung Bad Saulgau", "Schwäbische Zeitung Bad Waldsee", "Schwäbische Zeitung Biberach",
                            "Schwäbische Zeitung Ehingen", "Schwäbische Zeitung Friedrichshafen", "Schwäbische Zeitung Gränzbote", "Schwäbische Zeitung Laupheim",
                            "Schwäbische Zeitung Leutkirch", "Schwäbische Zeitung Ravensburg", "Schwäbische Zeitung Riedlingen", "Schwäbische Zeitung Sigmaringen/Meßkirch",
                            "Schwäbische Zeitung Tettnang", "Schwäbische Zeitung Wangen", "Zollern-Alb Kurier Schwäbische Zeitung"],
    "Schwarzwälder Bote": ["Schwarzwälder Bote Albstadt-Ebingen A1", "Schwarzwälder Bote Bad Wildbad C3", "Schwarzwälder Bote Bad Wildbad, Calw C2", 
                           "Schwarzwälder Bote Balingen A2", "Schwarzwälder Bote Donaueschingen/Blumberg B1-D", "Schwarzwälder Bote Freudenstädter Kreiszeitung F1",
                           "Schwarzwälder Bote Hechingen A3", "Schwarzwälder Bote Kreisnachrichten Calw C2", "Schwarzwälder Bote Nagold C1", "Schwarzwälder Bote Oberndorf R1",
                           "Schwarzwälder Bote Rottweil R2", "Schwarzwälder Bote St. Georgen, Triberg, Furtwangen B1-L"],
    "Stuttgarter Nachrichten": ["Marbacher Zeitung", "Stuttgarter Nachrichten Leonberg", "Stuttgarter Nachrichten Stadtausgabe Innenstadt"],
    "Stuttgarter Zeitung": ["Stuttgarter Zeitung D", "Stuttgarter Zeitung Filder-Zeitung", "Stuttgarter Zeitung Ludwigsburg Stadt und Kreis", 
                            "Stuttgarter Zeitung Stadtausgabe Innenstadt"],
    "Südkurier": ["Alb Bote (Südkurier)", "Südkurier Bad Säckingen Wehr Rheinfelden SLR", "Südkurier Bodenseekreis Markdorf/Friedrichshafen", 
                  "Südkurier Donaueschinger Zeitung DNE", "Südkurier Konstanz K", "Südkurier Meßkirch Pfullendorf MP", "Südkurier Radolfzeller Zeitung R", 
                  "Südkurier Singener Zeitung H", "Südkurier Stockach RS", "Südkurier Villingen Schwenningen Schwarzwald"],
    "ZfK Zeitung für Kommunale Wirtschaft": ["ZfK - Zeitung für kommunale Wirtschaft", "ZfK Zeitung für kommunale Wirtschaft"]
}

editorials = {
    "Badische Neueste Nachrichten Redaktion": ["Badisches Tagblatt Redaktion", "Badische Neueste Nachrichten Redaktion Ettlingen", 
                                               "Badische Neueste Nachrichten Hardt Redaktion", 
                                     "Badische Neueste Nachrichten Redaktion", "Badische Neueste Nachrichten Baden-Baden", "Badische Neueste Nachrichten Redaktion",
                                     "Brettener Nachrichten Redaktion", "Bruchsaler Rundschau Redaktion"],
    "Badische Zeitung Redaktion": ["Badische Zeitung", "Redaktion Badische Zeitung Bad Säckingen", "Badische Zeitung Elztal Redaktion", "Badische Zeitung Emmendingen Redaktion", 
                                   "Badische Zeitung Redaktion", "Badische Zeitung Redaktion", "Redaktion Badische Zeitung Hochschwarzwald", "Redaktion Badische Zeitung Lörrach", "Badische Zeitung Breisgau Redaktion"],
    "Badisches Tagblatt Redaktion": ["Badisches Tagblatt Redaktion Murgtäler", "Badisches Tagblatt Redaktion Rastatt", "Badisches Tagblatt Redaktion", 
                                     "Badisches Tagblatt Redaktion Rastatt"],
    #"Bild Redaktion": ["Auto Bild Redaktion", "Bild am Sonntag Redaktion", "Bild Bremen Redaktion", "Bild Redaktion Berlin-Brandenburg", "Bild Stuttgart Redaktion"],
    #"DIE WELT Redaktion": ["DIE WELT Redaktion", "DIE WELT Redaktion", "Welt am Sonntag Redaktion Hamburg", "Welt am Sonntag Redaktion Hamburg", "DIE WELT Redaktion"],
    #"Euro Redaktion": ["Euro Redaktion", "Euro am Sonntag Redaktion"], 
    #"Focus Redaktion": ["Focus Redaktion", "Focus Money Redaktion"],
    "Frankfurter Allgemeine Redaktion": ["Frankfurter Allgemeine Sonntagszeitung Redaktion", "Frankfurter Allgemeine Redaktion"],
    "Nordwest-Zeitung Redaktion": ["Der Münsterländer Redaktion", "Nordwest-Zeitung Redaktion"],
    "Rheinische Post Redaktion": ["Rheinische Post Redaktion", "Rheinische Post Erkelenz Redaktion", "Rheinische Post Redaktion Mönchengladbach", 
                                  "Neuss-Grevenbroicher Zeitung Redaktion"],
    "Rhein-Zeitung Redaktion": ["Rhein-Zeitung Redaktion Cochem", "Rhein-Zeitung Redaktion"],
    "Schwäbische Zeitung Redaktion": ["Schwäbische Zeitung Laichingen Redaktion", "Schwäbische Zeitung Saulgau Redaktion", "Schwäbische Zeitung Bad Waldsee Redaktion", 
                                      "Schwäbische Zeitung Biberach Redaktion", "Schwäbische Zeitung Ehingen Redaktion", "Schwäbische Zeitung Friedrichshafen Redaktion",
                                      "Gränzbote Redaktion", "Schwäbische Zeitung Laupheim Redaktion", "Schwäbische Zeitung Leutkirch Redaktion", 
                                      "Schwäbische Zeitung Ravensburg Redaktion", "Schwäbische Zeitung Riedlingen Redaktion", "Schwäbische Zeitung Sigmaringen Redaktion", 
                                      "Schwäbische Zeitung Tettnang Redaktion", "Schwäbische Zeitung Wangen Redaktion", "Zollern-Alb Kurier Redaktion"],
    "Schwarzwälder Bote Redaktion": ["Schwarzwälder Bote Albstadt-Ebingen Redaktion", "Schwarzwälder Bote Bad Wildbad Redaktion", "Schwarzwälder Bote Bad Wildbad Redaktion",
                                     "Schwarzwälder Bote Balingen Redaktion", "Schwarzwälder Bote Schwarzwald Redaktion", "Freudenstädter Kreiszeitung Redaktion",
                                     "Schwarzwälder Bote Hechingen Redaktion", "Kreisnachrichten Calw Redaktion", "Schwarzwälder Bote Gesellschafter Redaktion", 
                                     "Schwarzwälder Bote Redaktion", "Rottweiler Zeitung Redaktion", "Schwarzwälder Bote Schwarzwald Redaktion"],
    "Stuttgarter Nachrichten Redaktion": ["Marbacher Zeitung Redaktion", "Stuttgarter Nachrichten Redaktion"],
    "Stuttgarter Zeitung Redaktion": ["Stuttgarter Zeitung Redaktion", "Filder Zeitung Redaktion", "Stuttgarter Zeitung Kreis Ludwigsburg Redaktion", 
                                      "Stuttgarter Zeitung Redaktion"],
    "Südkurier Redaktion": ["Alb Bote Redaktion", "Bad Säckinger Zeitung Redaktion", "Südkurier Bodensee Redaktion Markdorf", "Donaueschinger Zeitung Redaktion", 
                  "Südkurier Redaktion", "Südkurier Meßkircher Zeitung Redaktion", "Radolfzeller Zeitung Redaktion", "Singener Zeitung Redaktion", "Südkurier Redaktion Stockach",
                  "Villinger Nachrichten Redaktion"]
}

publisher_title = {
    "Der neue Tag - Oberpfälzischer Kurier Druck- u. Verlagshaus GmbH": "DER NEUE TAG Großlandkreis Schwandorf",
    "Fränkische Nachrichten Verlags-GmbH": ["Fränkische Nachrichten Bad Mergentheim", "Fränkische Nachrichten Buchen Walldürn"],
    "Verlagsgesellschaft Madsack GmbH & Co.KG":	"Hannoversche Allgemeine Zeitung Hemmingen",
    "Heilbronner Stimme GmbH & Co. KG":	"Heilbronner Stimme",
    "M. DuMont Schauberg GmbH & Co.KG":	"Kölnische Rundschau",
    "Mannheimer Morgen Großdruckerei und Verlag GmbH": "Mannheimer Morgen Stadtausgabe",
    "Ostsee-Zeitung GmbH & Co.KG": "Ostsee-Zeitung Rostocker Zeitung",
    "Rhein-Neckar Zeitung GmbH": "Rhein-Neckar-Zeitung Nordbadische Nachrichten",
    "The Times": "THE TIMES",
    "VKU Verlag GmbH": "ZfK Zeitung für Kommunale Wirtschaft"
}

def normalize_data(df):
    """
    This function normalizes the values in the columns 'media_title' and 'media_editorial' which are meant to be the same despite different spelling

    args:
        df: The Dataframe

    Returns:
        df: The Dataframe after being normalized
    """
    normalized_title = col("media_title")
    normalized_editorial = col("media_editorial")

    for key, values in titles.items():
        for value in values:
            normalized_title = when(col("media_title") == value, key).otherwise(normalized_title)

    df = df.withColumn("media_title", normalized_title)

    for key, values in editorials.items():
        for value in values:
            normalized_editorial = when(col("media_editorial") == value, key).otherwise(normalized_editorial)

    df = df.withColumn("media_editorial", normalized_editorial)

    return df

df_argus_print = spark.read.table("datif_pz_uk_dev.03_transformed.argus_print_media_panel")

df_argus_print = df_argus_print.select(
    F.col("clipping_id"),
    F.col("clipping_headline"),
    F.col("clipping_publication_date"),
    F.col("clipping_abstract"),
    F.col("media_title"),
    F.col("media_editorial"),
    F.col("media_publisher"),
    F.col("media_gross_reach"),
    F.col("media_print_run"),
    F.col("media_copies_sold"),
    F.col("media_copies_distributed"),
    F.col("media_sector"),
    F.col("media_state"),
    F.col("media_type")
)

#df_argus_print = normalize_data(df_argus_print)

def fill_genios_print(df):
    """
    This function fills the Genios data by adding new columns and values to the columns 'media_publisher', 'media_title', 'media_editorial' in case there is a match with the attribute 'Source'

    """
    df = df.withColumn("media_publisher", lit(None))\
            .withColumn("media_title", lit(None))\
            .withColumn("media_editorial", lit(None))\
            .withColumn("media_gross_reach", lit(None))\
            .withColumn("media_print_run", lit(None))\
            .withColumn("media_copies_sold", lit(None))\
            .withColumn("media_copies_distributed", lit(None))\
            .withColumn("media_sector", lit(None))\
            .withColumn("media_state", lit(None))\
            .withColumn("media_type", lit(None))

    for key, value in publisher_title_editorial.items():
        df = df.withColumn("media_publisher", 
                           when(col("Source") == key, lit(value[0]))
                           .otherwise(col("media_publisher")))
        df = df.withColumn("media_title", 
                           when(col("Source") == key, lit(value[1]))
                            .otherwise(col("Source")))
        df = df.withColumn("media_editorial", 
                           when(col("Source") == key, lit(value[2]))
                           .otherwise(col("media_editorial")))
    
    return df

def fill_null_editorials(df):
    """
    This function fills null values in the column 'media_editorials'
    """
    df = df.withColumn("media_editorial",
                       when((col("media_editorial").isNull()) & (col("media_title").contains("DER NEUE TAG")), lit("Der Neue Tag Redaktion"))
                        .when((col("media_editorial").isNull()) & (col("media_title") == "Fränkische Nachrichten Buchen Walldürn"), lit("Fränkische Nachrichten Buchen Redaktion"))
                        .when((col("media_editorial").isNull()) & (col("media_title").contains("Hannoversche Allgemeine Zeitung")), lit("Nordhannoversche Zeitung Redaktion"))
                        .when((col("media_editorial").isNull()) & (col("media_title") == "Heilbronner Stimme"), lit("Heilbronner Stimme Redaktion"))
                        .when((col("media_editorial").isNull()) & (col("media_title") == "Kölnische Rundschau" ), lit("Kölnische Rundschau Redaktion"))
                        .when((col("media_editorial").isNull()) & (col("media_title") == "Märkische Oderzeitung Spree-Journal Erkner"), lit("Märkisches Oderzeitung Redaktion Erkner"))
                        .when((col("media_editorial").isNull()) & (col("media_title") == "Ostsee-Zeitung Rostocker Zeitung" ), lit("Ostsee-Zeitung Redaktion"))
                        .when((col("media_editorial").isNull()) & (col("media_title") == "Rhein-Neckar-Zeitung Nordbadische Nachrichten"), lit("Nordbadische Nachrichten Redaktion"))
                        .when((col("media_editorial").isNull()) & (col("media_title") == "Südwest Presse Ehingen"), lit("Ehingen Redaktion"))
                        .when((col("media_editorial").isNull()) & (upper(col("media_title")) == "THE TIMES" ), lit("The Times Redaktion"))
                        .when((col("media_editorial").isNull()) & (col("media_title") == "ZfK Zeitung für Kommunale Wirtschaft"), lit("ZFK Zeitung für Kommunale Wirtschaft"))
                        .otherwise(col("media_editorial"))                          
    )
    
    # df = df.withColumn("media_editorial", 
    #                    when(col("media_editorial").isNull(), concat(col("media_title"), lit(" Redaktion")))
    #                     .otherwise(col("media_editorial")))
 
    return df

def fill_null_publishers(df):
    """
    This function fills null values in the column 'media_publisher'
    """
    for publisher, titles in publisher_title.items(): 
            df = df.withColumn("media_publisher",
                       when((col("media_publisher").isNull()) & (col("media_title").isin(titles)), lit(publisher))
                        .otherwise(col("media_publisher")))
    
    return df

df_genios_print = spark.read.table("datif_pz_uk_dev.03_transformed.genios_enbw_print_media_panel")

df_genios_print = fill_genios_print(df_genios_print)

df_genios_print = df_genios_print.select(
    F.col("DocID").alias("clipping_id"),
    F.col("Title").alias("clipping_headline"),
    F.col("PublishingDay").alias("clipping_publication_date"),
    F.col("Abstract").alias("clipping_abstract"),
    F.col("media_title"),
    F.col("media_editorial"),
    F.col("media_publisher"),
    F.col("media_gross_reach"),
    F.col("media_print_run"),
    F.col("media_copies_sold"),
    F.col("media_copies_distributed"),
    F.col("media_sector"),
    F.col("media_state"),
    F.col("media_type")
)

df_joined_print = df_genios_print.union(df_argus_print)
df_joined_print = normalize_data(df_joined_print)
df_joined_print = fill_null_publishers(df_joined_print)
df_joined_print = fill_null_editorials(df_joined_print)
display(df_joined_print)

fn_overwrite_table(df_joined_print, target_schema_name=target_schema_name, target_table_name="genios_argus_print_media", target_path=target_path)

# COMMAND ----------

schema = StructType([
    StructField("Werbeträger", StringType(), True),
    StructField("Mio.", DoubleType(), True),
    StructField("% vert.", DoubleType(), True),
    StructField("Fälle", IntegerType(), True)
])

data = []

df_14_19 = spark.createDataFrame(data, schema)
df_20_24 = spark.createDataFrame(data, schema)
df_25_29 = spark.createDataFrame(data, schema)
df_30_44 = spark.createDataFrame(data, schema)
df_45_59 = spark.createDataFrame(data, schema)

display(df_14_19)
