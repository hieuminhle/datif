# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Anbindung Eco Journal Themen 
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC - Die UK bekommt Marktforschungsdaten von Mindline (Reputationsdaten, Markenbekanntheit etc), erstelle eine consolidierte Tabelle aus den Marktforschungsdaten
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
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_gestuetzte_markenbekanntheit(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_reputationsindex(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_ungestuetzte_A_themen(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_ungestuetzte_energie_themen(_bawue)
# MAGIC - datif_pz_uk_{}.03_cleaned.mkm_ungestuetzte_markenbekanntheit(_bawue)
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 19.06.2025 Max Mustermann: init

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# Define schema name and path to store tables
target_schema_name = "03_transformed"
target_path = "mkm_marktforschungsdaten"

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, when, col, to_date, expr
import pyspark.sql

# COMMAND ----------



# COMMAND ----------

def RenameFirstColumn(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Ersetzt den Namen der ersten Spalte in Kategorie


    Input:
    - df: Eingehende DataFrame

    Output:
    - df_renamed: DataFrame mit umbenannter ersten Spalte

    """
    df_renamed = df.withColumnRenamed(df.columns[0], "Kategorie")
    return df_renamed

# COMMAND ----------

def deleteNullKategorie(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Löscht Zeilen mit Null-Werten in der Kategorie-Spalte

    Input:
    - df: Eingehende DataFrame

    Output:
    - df_null_deleted: DataFrame ohne Zeilen mit Null-Werten in der Kategorie-Spalte
    """
    df_null_deleted = df.filter(col("Kategorie").isNotNull())
    return df_null_deleted

# COMMAND ----------

def unpivot(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Entpivotiert eine DataFrame-Spalte mit mehreren Werten in eine Spalte mit Werten und eine Spalte mit den jeweiligen Schlüsseln.

    Input:
    - df: Eingehende DataFrame

    Output:
    - df_unpivoted: DataFrame mit entpivotierter Spalte

    """

    
    # Erste Spalte (nicht entpivotisieren)
    id_column = df.columns[0]  # "Kategorie"

    # Alle anderen Spalten (werden entpivotisiert)
    value_columns = df.columns[1:]

    # Stack-Expression mit SQL-kompatibler Cast-Syntax
    stack_expr = ", ".join([f"'{col}', CAST(`{col}` AS STRING)" for col in value_columns])

    # Anzahl der Spaltenpaare
    n = len(value_columns)

    # Entpivotisierung mit selectExpr
    df_unpivoted = df.selectExpr(
        f"`{id_column}` as Kategorie",
        f"stack({n}, {stack_expr}) as (Datum, Wert)"
    )

    return df_unpivoted


# COMMAND ----------

from pyspark.sql.functions import when, col, regexp_replace, to_date

def date_mapping(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Wandelt Datumsspalte im Format '2023_Januar' in echtes Datum '2023-01-01' um.
    """
    monat_map = {
        "Januar": "01",
        "Februar": "02",
        "März": "03",
        "April": "04",
        "Mai": "05",
        "Juni": "06",
        "Juli": "07",
        "August": "08",
        "September": "09",
        "Oktober": "10",
        "November": "11",
        "Dezember": "12"
    }

    # Starte mit Originalspalte
    df_mapped = df.withColumn("Datum_str", col("Datum"))

    # Wende alle Ersetzungen an (Kaskade von when())
    for name, num in monat_map.items():
        df_mapped = df_mapped.withColumn(
            "Datum_str",
            when(col("Datum_str").endswith(name),
                 regexp_replace("Datum_str", f"_{name}$", f"-{num}-01")
            ).otherwise(col("Datum_str"))
        )

    # Konvertiere String zu echtem Datum
    df_mapped = df_mapped.withColumn("Datum", to_date("Datum_str", "yyyy-MM-dd"))

    # Optional: Hilfsspalte entfernen
    df_mapped = df_mapped.drop("Datum_str")

    return df_mapped

# COMMAND ----------

table_names = [
    "mkm_gestuetzte_markenbekanntheit",
    "mkm_gestuetzte_markenbekanntheit_bawue",
    "mkm_reputationsindex",
    "mkm_reputationsindex_bawue",
    "mkm_ungestuetzte_A_themen",
    "mkm_ungestuetzte_A_themen_bawue",
    "mkm_ungestuetzte_energie_themen",
    "mkm_ungestuetzte_energie_themen_bawue",
    "mkm_ungestuetzte_markenbekanntheit",
    "mkm_ungestuetzte_markenbekanntheit_bawue"
]

# COMMAND ----------

for name in table_names:
    try:
        print(f"Verarbeite Tabelle: {name}")
        
        df_view = spark.table(f"datif_pz_uk_{env}.`02_cleaned`.`{name}`")

        df_renamed = RenameFirstColumn(df_view)
        deleteNullKategoried = deleteNullKategorie(df_renamed)
        df_unpivoted = unpivot(deleteNullKategoried)
        df_mapped = date_mapping(df_unpivoted)
        df_mapped = df_mapped.withColumn("Wert", col("Wert").cast("integer"))

        table_name_cleaned = name.replace("-", "_").lower()
        display(table_name_cleaned)

        fn_overwrite_table(
            df_source=df_mapped,
            target_schema_name=target_schema_name,
            target_table_name=table_name_cleaned,
            target_path=target_path
        )

        print(f"Gespeichert: {table_name_cleaned}")


    except Exception as e:
        print(f"Fehler bei {name}: {e}")

