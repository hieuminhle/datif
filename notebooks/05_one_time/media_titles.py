# Databricks notebook source
# Laden der Daten
df = (
    spark.read.table("datif_pz_uk_dev.03_transformed.argus_print_media_panel")
    .select("media_title", "media_editorial", "media_publisher", "media_type", "media_state", "media_gross_reach")
    .distinct()
)

df.show(truncate=False)

# COMMAND ----------

# Datei in Databricks tmp folder ablegen
output_path = "/dbfs/tmp/unique_argus_print_media_panel.csv"
df.write.mode("overwrite").csv(output_path, header=True)

print(f"CSV-Datei gespeichert unter: {output_path}")
