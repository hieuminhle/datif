# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Anbindung Eco Journal Themen 
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC - Die UK bekommt Marktforschungsdaten von Mindline (Reputationsdaten, Markenbekanntheit etc), diese liegen in 03_tranformed und sollen in 04_PowertBiMart
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_gestuetzte_markenbekanntheit(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_reputationsindex(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_ungestuetzte_A_themen(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_ungestuetzte_energie_themen(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_ungestuetzte_markenbekanntheit(_bawue)
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - datif_pz_uk_{}.04_power_bi_mart.mkm_consolidate
# MAGIC - datif_pz_uk_{}.04_cleaned.mkm_gestuetzte_markenbekanntheit(_bawue)
# MAGIC - datif_pz_uk_{}.04_cleaned.mkm_reputationsindex(_bawue)
# MAGIC - datif_pz_uk_{}.04_cleaned.mkm_ungestuetzte_A_themen(_bawue)
# MAGIC - datif_pz_uk_{}.04_cleaned.mkm_ungestuetzte_energie_themen(_bawue)
# MAGIC - datif_pz_uk_{}.04_cleaned.mkm_ungestuetzte_markenbekanntheit(_bawue)
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 25.09.2025 Minh Hieu Le: Sicherstellen, dass in den Zahlenspalten keine Nullwerte vorkommen
# MAGIC - 04.09.2025 Minh Hieu Le: Anpassen, sodass redudante Tabellen nicht mehr geschrieben werden
# MAGIC - 19.06.2025 Max Mustermann: init

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

pz_target_schema_name = "04_power_bi_mart"
target_path = "mkm_marktforschungsdaten"

# COMMAND ----------

table_names = [
    # "mkm_gestuetzte_markenbekanntheit",
    # "mkm_gestuetzte_markenbekanntheit_bawue",
    # "mkm_reputationsindex",
    # "mkm_reputationsindex_bawue",
    # "mkm_ungestuetzte_A_themen",
    # "mkm_ungestuetzte_A_themen_bawue",
    # "mkm_ungestuetzte_energie_themen",
    # "mkm_ungestuetzte_energie_themen_bawue",
    # "mkm_ungestuetzte_markenbekanntheit",
    # "mkm_ungestuetzte_markenbekanntheit_bawue",
    "mkm_consolidate"
]

# COMMAND ----------

for name in table_names:
    try:
        print(f"🔄 Verarbeite: {name}")

        # Tabelle aus transformed-View lesen
        df = spark.sql(f"""
            SELECT * 
            FROM datif_pz_uk_{env}.`03_transformed`.`{name}`
        """)

        if "Wert" in df.columns:
            df = df.withColumn("Wert", F.coalesce(F.col("Wert"), F.lit(0)))

        # Zieldateiname ohne Bindestriche
        table_name_cleaned = name.replace("-", "_").lower()

        # Speichern als Tabelle
        fn_overwrite_table(
            df_source=df,
            target_schema_name=pz_target_schema_name,
            target_table_name=table_name_cleaned,
            target_path=target_path
        )

        print(f"✅ Gespeichert: {table_name_cleaned}")

    except Exception as e:
        print(f"❌ Fehler bei {name}: {e}")

