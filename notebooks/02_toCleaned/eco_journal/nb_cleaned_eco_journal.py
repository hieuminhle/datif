# Databricks notebook source
# MAGIC %md
# MAGIC # UK - Anbindung Eco Journal Themen 
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):
# MAGIC - Für die Unternehmenskommunikation, sind die Website Performaneces des Eco Journals von Bedeutung. Da rein anhand der URLs Eco Journals nicht erkenntlich sind, werden alle relevanten URLs die analysiert werden sollen, über eine Liste bereitgestellt. Mit diesem notebook wird eine Tabelle der Liste von URLs und den strategischen Themen Tags der Webseite angefertigt. 
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - Data Storage: 
# MAGIC - PZ-UK-01-Raw: eco_journal/27-02-2024_ECOJournal_Contents_Übersicht.xlsxeco_journal_content_table
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - Unity-Catalog:
# MAGIC - datif_pz_uk_{}.02_cleaned.eco_journal_content_table
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):
# MAGIC - 03.06.2025 Max Mustermann: Add Eco*Journal Strategische Themen
# MAGIC - 24.04.2025 Max Mustermann: Add Eco*Journal Abstract
# MAGIC - 12.03.2025 Max Mustermann: Init

# COMMAND ----------

# MAGIC %pip install openai==1.55.3

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

import os
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import lit, collect_list, concat_ws, regexp_replace, to_date, coalesce, date_format, col
from functools import reduce
from openai import AzureOpenAI
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from pyspark.sql import SparkSession

# COMMAND ----------

# Define schema name and path to store tables
target_schema_name = "02_cleaned"
target_path = "eco_journal"
target_table_name="eco_journal_content_table"
# Set source and trg path
source_path = sta_endpoint_pz_uk["01_raw"] + "/eco_journal"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Webscraping

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fuction to get the relevant text from the url

# COMMAND ----------

def get_text_from_url(url: str) -> List:
    """
    This function extracts the relevant text from a given URL and returns a list containing dicts with the titles and the paragraphs.
    Parameters:
        url: The URL of the Eco*Journal article.
    Returns:
        A list of dictionaries, where each dictionary contains a title and a list of paragraphs.
    """

    # Get the HTML content of the URL
    r = requests.get(url)
    r.encoding = 'utf-8'
    soup = BeautifulSoup(r.text, 'html.parser')

    # Liste für die relevanten Texte einsammeln
    sections = []

    # Einleitung
    h1 = soup.find('h1')
    first_p = h1.find_next('p') if h1 else None
    if h1 and first_p:
        sections.append({
            'title': h1.get_text(strip=True),
            'paragraphs': [first_p.get_text(strip=True)]
            })

    # Wir suchen alle h2, dann die jeweils *nächstfolgende* div.rich-text
    h2_tags = soup.find_all('h2')
    for h2 in h2_tags:
        # Nachfolgende Elemente durchsuchen bis wir auf das nächste passende div.rich-text stoßen
        next_div = None
        for sibling in h2.find_all_next():
            if sibling.name == 'h2':
                # nächster Abschnitt beginnt, also abbrechen
                break
            if sibling.name == 'div' and 'rich-text' in sibling.get('class', []):
                next_div = sibling
                break

        if next_div:
            paragraphs = [p.get_text(strip=True) for p in next_div.find_all('p')]
            if paragraphs:  # nur aufnehmen, wenn auch Inhalt da ist
                sections.append({
                    'title': h2.get_text(strip=True),
                    'paragraphs': paragraphs
                })
        

    try:
        return sections
    except Exception as e:
        print(f"Error: {e}")
        return []


# COMMAND ----------

# MAGIC %md
# MAGIC ### Function to add Abstract in needed Row

# COMMAND ----------

def fetch_or_reuse_abstract(row: DataFrame) -> str:
    """
    Function that fetches or reuses the abstract for a given row if needed.
    Parameters:
        row: Merged Dataframe from 01_raw.eco_journal and 02_cleaned.eco_journal_content_table .
    Returns:
            The abstract for the needed row.
    """
    # Prüfen, ob Abstract bereits sinnvoll gefüllt ist (nicht NaN, nicht leer, nicht nur Whitespaces)
    if isinstance(row['Abstract'], str) and row['Abstract'].strip():
        return row['Abstract']
    # Wenn kein Abstract vorhanden ist wird er für diese URL neu generiert
    try:
        sections = get_text_from_url(row['URL'])
        abstract = generate_abstract(sections, deployment)
        return abstract
    except Exception as e:
        print(f"Fehler bei URL {row['URL']}: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Created LLM to generate Abstract

# COMMAND ----------

AZURE_OPENAI_API_KEY = get_secret('openai-key')
AZURE_OPENAI_ENDPOINT = get_secret('openai-endpoint')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function for abstract generation

# COMMAND ----------

class UnscorableCommentError(Exception):
    pass

#  Function: Call LLM for abstract generation
def generate_abstract(s: str, model: str) -> str:
    messages = generate_prompt_messages(s)
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        seed=42,
        max_tokens=800,
        temperature=0.0,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None,
        stream=False
    )
    abstract = response.choices[0].message.content.strip()
    # Raise an Attribute Error if abstract could not be generated
    try:
        return str(abstract)
    except AttributeError:
        raise UnscorableCommentError(f"Could not generate abstract: {s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function with systempromt as message und relevant text from url

# COMMAND ----------

def generate_prompt_messages(s: str) -> List[Dict]:
    return [
        {
            "role": "user",
            "content": f"""
                Du bist ein professioneller PR-Redakteur mit Erfahrung in Unternehmenskommunikation, insbesondere im Energiesektor.
                Schreibe einen Abstract des folgenden Artikels. Der Abstract soll maximal 1.000 Zeichen lang sein.
                Ziel ist es, die zentralen Aussagen, Projekte, Ziele und Innovationsaspekte aus PR-Sicht kompakt und verständlich zusammenzufassen.
                Achte darauf, relevante Zahlen, Zeitangaben (z. B. bis 2025), Projektbeteiligungen und Alleinstellungsmerkmale der A hervorzuheben.
                Der Stil soll professionell, sachlich und dennoch leicht verständlich sein – wie für eine Pressemitteilung oder einen Medienüberblick.
                Gib nur den Abstract als Ergebnis zurück. Keine Einleitung, keine Metakommentare.
            
            Dies ist der Text:
            
            {s}
            """.strip(),
        },
    ]

# COMMAND ----------

client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    #api_version="2024-02-01",
    api_version="2024-08-01-preview",
    azure_endpoint = AZURE_OPENAI_ENDPOINT
    )
    
deployment='gpt-4'

# COMMAND ----------

# MAGIC %md
# MAGIC ##
# MAGIC  Create 02_cleaned_eco_journal_content_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### define the schemas

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("Headline", StringType(), True),
    StructField("URL", StringType(), True),
    StructField("Autor_in", StringType(), True),
    StructField("Veroeffentlicht", StringType(), True),
    StructField("Rubrik", StringType(), True),
    StructField("Tag_1", StringType(), True),
    StructField("Tag_2", StringType(), True),
    StructField("Tag_3", StringType(), True),
    StructField("Strategische_Themen", StringType(), True)
])

# COMMAND ----------

# Schema
schema_with_abstract = StructType([
    StructField("Headline", StringType(), True),
    StructField("URL", StringType(), True),
    StructField("Autor_in", StringType(), True),
    StructField("Veroeffentlicht", StringType(), True),
    StructField("Rubrik", StringType(), True),
    StructField("Tag_1", StringType(), True),
    StructField("Tag_2", StringType(), True),
    StructField("Tag_3", StringType(), True),
    StructField("Strategische_Themen", StringType(), True),
    StructField("Abstract", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Excel File and do a dataframe

# COMMAND ----------

# Read file from source path
file_info_list = dbutils.fs.ls(source_path)
for file_info in file_info_list:
    filename = file_info.name
    # Check if 'TAB' is in the filename and if it is an Excel file
    if filename.endswith(('.xlsx', '.xls')):
        filepath = os.path.join(source_path, filename)
        try:
            # Read Table B1 Excel file
            df_eco_journal = spark.read.format("com.crealytics.spark.excel") \
                            .option("dataAddress", "'veröffentlichte Contents'!A4:I98") \
                            .option("treatEmptyValuesAsNulls", "true") \
                            .option("header", "true") \
                            .option("inferSchema", "true") \
                            .schema(schema) \
                            .load(filepath)
            print(f"Data from {filename} has been processed.")

            df_eco_journal = df_eco_journal.withColumn("Veroeffentlicht", coalesce(to_date("Veroeffentlicht", "dd.MM.yyyy"), to_date("Veroeffentlicht", "M/d/yy"))) \
                                           .withColumn("URL", regexp_replace(col("URL"), " ", ""))
            
            # fn_overwrite_table(df_source=df_eco_journal, target_schema_name=target_schema_name, target_table_name="eco_journal_content_table", target_path=target_path)
        
        except Exception as e:
            print(e)
df_raw_pd = df_eco_journal.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # Raise Error Duplicate

# COMMAND ----------

# Wenn in df_raw_pd URL Dublilate hat dann raise error
if df_raw_pd['URL'].duplicated().any():
    raise ValueError("URLs in df_raw_pd sind nicht eindeutig.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Get 02_cleaned_eco_journal_content_table

# COMMAND ----------

df_existing = spark.table(f"{target_schema_name}.{target_table_name}").toPandas()
print("📄 Vorhandene Tabelle geladen")

# Sicherheitscheck: Fehlt die Spalte "Abstract"? Dann hinzufügen
if "Abstract" not in df_existing.columns:
    df_existing["Abstract"] = None
    print("⚠️ Spalte 'Abstract' wurde in bestehender Tabelle ergänzt.")

# Nur die relevanten Spalten URL und Abstract von der bestehenden Tabelle speichern
df_existing_subset = df_existing[['URL', 'Abstract']]

df_existing_subset = df_existing_subset.drop_duplicates(subset="URL").reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge 01_raw and 02_cleaned eco journal Data

# COMMAND ----------

df_combined = pd.merge(df_raw_pd, df_existing_subset, on="URL", how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add the Merged Dataframe the needed abstract 

# COMMAND ----------

tqdm.pandas()
df_combined["Abstract"] = df_combined.progress_apply(fetch_or_reuse_abstract, axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update 02_cleaned.eco_journal_content_table

# COMMAND ----------


df_final = spark.createDataFrame(df_combined[schema_with_abstract.fieldNames()])
fn_overwrite_table(
    df_source=df_final,
    target_schema_name=target_schema_name,
    target_table_name=target_table_name,
    target_path=target_path
)
print("✅ Tabelle erfolgreich aktualisiert.")
