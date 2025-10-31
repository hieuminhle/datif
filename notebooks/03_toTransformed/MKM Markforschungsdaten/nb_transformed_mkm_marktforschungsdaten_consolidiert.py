# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Anbindung Eco Journal Themen 
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC - Die UK bekommt Marktforschungsdaten von Mindline (Reputationsdaten, Markenbekanntheit etc), diese liegen in 02_cleaned und sollen in 03_tranformed
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_gestuetzte_markenbekanntheit(_bawue)
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_reputationsindex(_bawue)
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_ungestuetzte_enbw_themen.csv(_bawue)
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_ungestuetzte_energie_themen(_bawue)
# MAGIC - datif_pz_uk_{}.02_cleaned.mkm_ungestuetzte_markenbekanntheit(_bawue)
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_gestuetzte_markenbekanntheit(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_reputationsindex(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_ungestuetzte_enbw_themen(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_ungestuetzte_energie_themen(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_ungestuetzte_markenbekanntheit(_bawue)
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 19.06.2025 Justin Stange-Heiduk: init

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# Define schema name and path to store tables
target_schema_name = "03_transformed"
target_path = "mkm_marktforschungsdaten"

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, when, col, to_date, expr
import pyspark.sql
import pandas as pd

# COMMAND ----------

table_names = [
    "mkm_gestuetzte_markenbekanntheit",
    "mkm_gestuetzte_markenbekanntheit_bawue",
    "mkm_reputationsindex",
    "mkm_reputationsindex_bawue",
    "mkm_ungestuetzte_enbw_themen",
    "mkm_ungestuetzte_enbw_themen_bawue",
    "mkm_ungestuetzte_energie_themen",
    "mkm_ungestuetzte_energie_themen_bawue",
    "mkm_ungestuetzte_markenbekanntheit",
    "mkm_ungestuetzte_markenbekanntheit_bawue"
]

Mapping_dict = {
    "mkm_reputationsindex" :  ["Reputationsindex", "National", "Existier nicht"],
    "mkm_reputationsindex_bawue" : ["Reputationsindex", "Baden-Württemberg", "Existier nicht"],
    "mkm_gestuetzte_markenbekanntheit":  ["Markenbekanntheit", "National", "Gestützt"],
    "mkm_gestuetzte_markenbekanntheit_bawue":  ["Markenbekanntheit", "Baden-Württemberg", "Gestützt"],
    "mkm_ungestuetzte_enbw_themen":  ["EnBW Themen", "National", "Ungestützt"],
    "mkm_ungestuetzte_enbw_themen_bawue": ["EnBW Themen", "Baden-Württemberg", "Ungestützt"],
    "mkm_ungestuetzte_energie_themen":  ["Energie Themen", "National", "Ungestützt"],
    "mkm_ungestuetzte_energie_themen_bawue": ["Energie Themen","Baden-Württemberg", "Ungestützt"],
    "mkm_ungestuetzte_markenbekanntheit":  ["Markenbekanntheit", "National", "Ungestützt"],
    "mkm_ungestuetzte_markenbekanntheit_bawue": ["Markenbekanntheit", "Baden-Württemberg", "Ungestützt"]
}


# COMMAND ----------



dfs = []  # hier sammeln wir alle DataFrames

for name in table_names:
    try:
        print(f"Verarbeite Tabelle: {name}")

        
        df_view = spark.table(f"datif_pz_uk_{env}.`03_transformed`.`{name}`")
        df_view_pd = df_view.select("Kategorie", "Datum", "Wert").toPandas()

        df_view_pd["Inhalt"] = Mapping_dict[name][0]
        df_view_pd["Region"] = Mapping_dict[name][1]
        df_view_pd["UnGestuetzt"] = Mapping_dict[name][2]

        # Spaltenreihenfolge anpassen
        df_view_pd = df_view_pd[["Inhalt", "Region", "UnGestuetzt", "Kategorie", "Datum", "Wert"]]

        dfs.append(df_view_pd)

    except Exception as e:
        print(f"Fehler bei {name}: {e}")

# Alle zusammenführen
final_df = pd.concat(dfs, ignore_index=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabelle speichern

# COMMAND ----------

        final_spark_df = spark.createDataFrame(final_df)
        fn_overwrite_table(
            df_source=final_spark_df,
            target_schema_name=target_schema_name,
            target_table_name="mkm_consolidate",
            target_path=target_path
        )
