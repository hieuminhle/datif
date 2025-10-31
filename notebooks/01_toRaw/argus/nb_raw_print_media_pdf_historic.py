# Databricks notebook source
# MAGIC %md
# MAGIC # Argus - Abzug Volltexte von Print Medien PDFs
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):  
# MAGIC Abzug der PDF zu jedem Print Media Eintrag aus dem Abzug der Argus API und abspeichern der PDF in der PZ-UK.
# MAGIC Anschließend wird aus der PDF der Volltext gelesen und in einer JSON gespeichert.
# MAGIC Getriggert wird der Datenabzug und die Verarbeitung täglich über die ADF Pipeline '...'.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC -  datif_dz_dev.02_cleaned_uk_argus.argus_print_media_panel_historic
# MAGIC
# MAGIC * ZIEL:  
# MAGIC Storage: 
# MAGIC - 02-cleaned/argus/print_media_volltexte_historic/binary/_clipping_id_.pdf
# MAGIC - 02-cleaned/argus/print_media_volltexte_historic/json/_clipping_id_.json
# MAGIC
# MAGIC   
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):     
# MAGIC - 12.11.2024 Svenja Schuder: Init

# COMMAND ----------

# MAGIC %pip install azure-storage-file-datalake

# COMMAND ----------

# MAGIC %pip install PyMuPDF

# COMMAND ----------

import os
import re
import requests
import json
import pymupdf
from io import BytesIO
from azure.storage.filedatalake import DataLakeServiceClient

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Widgets

# COMMAND ----------

# Create a text input field for the entry date
dbutils.widgets.text("p_date", "yyyy/MM/dd HH:mm:ss")

# Get Entry Date from ADF Pipeline
p_date = dbutils.widgets.get("p_date")
date = p_date.split(" ")[0]
query_date = date.replace("/", "-")
time_part = p_date.split(" ")[1]
time = time_part.replace(":", "_")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Prameters

# COMMAND ----------

# Storage connection
pz_storage_name = get_secret("storage-datalake-name")
pz_storage_key = get_secret("storage-access-key")
connection_string = f'DefaultEndpointsProtocol=https;AccountName={pz_storage_name};AccountKey={pz_storage_key};EndpointSuffix=core.windows.net'

pz_container_name = "01-raw"

pz_sink_dir_binary = f"argus/print_media_volltexte_historic/binary"
pz_sink_dir_json = f"argus/print_media_volltexte_historic/json"

pdf_path = sta_endpoint_pz_uk['01_raw'] + '/' + pz_sink_dir_binary
json_path = sta_endpoint_pz_uk['01_raw'] + '/' + pz_sink_dir_json

# Create ADLS file system client
client = DataLakeServiceClient.from_connection_string(conn_str=connection_string)
file_system_client = client.get_file_system_client(pz_container_name)
directory_client = file_system_client.get_directory_client(pz_sink_dir_binary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Functions

# COMMAND ----------

def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Speichern der PDF im ADLS-PZ-UK-01-raw

# COMMAND ----------

# select records from DZ-02-cleaned schema
df_print_media_cleaned = spark.sql(f"""
                                   SELECT 
                                   clipping_id, 
                                   clipping_publication_date, 
                                   files_pdf_doc
                                   FROM datif_dz_dev.02_cleaned_uk_argus.argus_print_media_panel_historic
                                   --WHERE clipping_publication_date Like '{query_date}%'
                                   WHERE clipping_publication_date BETWEEN '2023-11-06' AND '2023-11-28' AND files_pdf_doc IS NOT NULL
                                   """)
# Create a temporary view of the DataFrame
df_print_media_cleaned.createOrReplaceTempView("print_media_cleaned")

# COMMAND ----------

display(df_print_media_cleaned)

# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame
pdf_df = df_print_media_cleaned.toPandas()

# Parse dictionary from the SQL query result
result_dict = pdf_df.to_dict(orient='records')

# COMMAND ----------

counter = 0
if len(result_dict) > 0:
    if path_exists(pdf_path)==True:
        existing_files = [file.name.split(".")[0] for file in dbutils.fs.ls(pdf_path)]
        for article in result_dict:
            if article['clipping_id'] in existing_files:
                print(f"Skipping {article['clipping_id']} because it already exists in the path.")
                continue
            else:
                print(f"Processing {article['clipping_id']}.")
                try:
                    clipping_id = article['clipping_id']
                    deeplink = article['files_pdf_doc']
                    deeplink = re.sub(r'dok/\d+/false/', 'web/api/export/v1/feed/dokument/', deeplink) + '?path=118465/Exp/Treffer/'

                    file_name = f'{clipping_id}.pdf'

                    # Download the PDF
                    response = requests.get(deeplink)
                    pdf_data = response.content

                    # Write PDF as file to ADLS 
                    file_client = directory_client.create_file(file_name)
                    file_client.append_data(data=BytesIO(pdf_data), offset=0, length=len(pdf_data))
                    file_client.flush_data(len(pdf_data))

                    print(f"PDF uploaded to ADLS at {pz_sink_dir_binary}/{file_name}")
                    counter = counter + 1

                except:
                    raise Exception("Failed to write data to storage.")
    else:
        try:
            for article in result_dict:
                print(f"Processing {article['clipping_id']}.")
                clipping_id = article['clipping_id']
                deeplink = article['files_pdf_doc']
                deeplink = re.sub(r'dok/\d+/false/', 'web/api/export/v1/feed/dokument/', deeplink) + '?path=118465/Exp/Treffer/'

                file_name = f'{clipping_id}.pdf'

                # Download the PDF
                response = requests.get(deeplink)
                pdf_data = response.content

                # Write PDF as file to ADLS 
                file_client = directory_client.create_file(file_name)
                file_client.append_data(data=BytesIO(pdf_data), offset=0, length=len(pdf_data))
                file_client.flush_data(len(pdf_data))

                print(f"PDF uploaded to ADLS at {pz_sink_dir_binary}/{file_name}")
                counter = counter + 1

        except:
            raise Exception("Failed to write data to storage.")
else:
    print("There are no new records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Speichern des Volltextes als JSON im ADLS
# MAGIC
# MAGIC

# COMMAND ----------

counter = 0
if len(result_dict) > 0:
    if path_exists(json_path)==True:
        existing_files = [json_file.name.split(".")[0] for json_file in dbutils.fs.ls(json_path)]
        for pdf_file in dbutils.fs.ls(pdf_path):
            if pdf_file.name.split(".")[0] in existing_files:
                print(f"Skipping {pdf_file.name} because it already exists in the path.")
                continue
            else:
                print(f"Processing {pdf_file.name}.")
                try:
                    json_pdf_content = dict()

                    # get clipping id from filename
                    clipping_id = os.path.splitext(pdf_file.name)[0]
                    json_pdf_content['clipping_id'] = clipping_id
                    
                    # download PDF file drom ADLS
                    file_client = directory_client.get_file_client(pdf_file.name)
                    download = file_client.download_file()
                    downloaded_bytes = download.readall()

                    # read text from pdf with PymuPDF
                    with pymupdf.open(stream=downloaded_bytes, filetype="pdf") as doc:

                        for page in doc:
                            rect = page.rect
                            height = 100
                            footer = 30
                            clip = pymupdf.Rect(0, height, rect.width, rect.height-footer)

                            if page.number == 0:
                                print(f'-------------- Processing Page {page.number} --------------')
                                full_text = chr(12).join([page.get_text(clip=clip)])
                                #json_pdf_content['Text'] = text

                                # Find tables in the page
                                tables = page.find_tables()
                                for table in tables:
                                    # Get the bounding box of the table
                                    table_bbox = table.bbox
                                    
                                    # Clip the area where the table lies and remove it from the full text
                                    table_text = page.get_text(clip=table_bbox)
                                    full_text = full_text.replace(table_text, "")
                                    
                                # Append the remaining text to the existing key in the dictionary
                                if 'Text' in json_pdf_content:
                                    json_pdf_content['Text'] += full_text
                                else:
                                    json_pdf_content['Text'] = full_text

                            else:
                                print(f'-------------- Processing Page {page.number} --------------')
                                clip = pymupdf.Rect(0, 0, rect.width, rect.height-footer)
                                full_text = chr(12).join([page.get_text(clip=clip)])

                                # Find tables in the page
                                tables = page.find_tables()
                                for table in tables:
                                    # Get the bounding box of the table
                                    table_bbox = table.bbox
                                    
                                    # Clip the area where the table lies and remove it from the full text
                                    table_text = page.get_text(clip=table_bbox)
                                    full_text = full_text.replace(table_text, "")
                                
                                # Append the remaining text to the existing key in the dictionary
                                json_pdf_content['Text'] += full_text 

                    # save JSON to ADLS 
                    json_dumps = json.dumps(json_pdf_content, ensure_ascii=False).encode('utf8')
                    dbutils.fs.put(sta_endpoint_pz_uk['01_raw'] + f"/{pz_sink_dir_json}/{clipping_id}.json", json_dumps.decode(), True)
                    print(f"{pdf_file.name} has succsessfully been processed and saved to {pz_sink_dir_json}/{clipping_id}.json")

                    counter = counter + 1
                
                except:
                    raise Exception("Failed to write data to storage.")
    else:
        try:
            for pdf_file in dbutils.fs.ls(pdf_path):
                print(f"Processing {pdf_file.name}.")
                json_pdf_content = dict()

                # get clipping id from filename
                clipping_id = os.path.splitext(pdf_file.name)[0]
                json_pdf_content['clipping_id'] = clipping_id
                
                # download PDF file drom ADLS
                file_client = directory_client.get_file_client(pdf_file.name)
                download = file_client.download_file()
                downloaded_bytes = download.readall()

                # read text from pdf with PymuPDF
                with pymupdf.open(stream=downloaded_bytes, filetype="pdf") as doc:

                    for page in doc:
                        rect = page.rect
                        height = 100
                        footer = 30
                        clip = pymupdf.Rect(0, height, rect.width, rect.height-footer)

                        if page.number == 0:
                            print(f'-------------- Processing Page {page.number} --------------')
                            full_text = chr(12).join([page.get_text(clip=clip)])
                            #json_pdf_content['Text'] = text

                            # Find tables in the page
                            tables = page.find_tables()
                            for table in tables:
                                # Get the bounding box of the table
                                table_bbox = table.bbox
                                
                                # Clip the area where the table lies and remove it from the full text
                                table_text = page.get_text(clip=table_bbox)
                                full_text = full_text.replace(table_text, "")
                                
                            # Append the remaining text to the existing key in the dictionary
                            if 'Text' in json_pdf_content:
                                json_pdf_content['Text'] += full_text
                            else:
                                json_pdf_content['Text'] = full_text

                        else:
                            print(f'-------------- Processing Page {page.number} --------------')
                            clip = pymupdf.Rect(0, 0, rect.width, rect.height-footer)
                            full_text = chr(12).join([page.get_text(clip=clip)])

                            # Find tables in the page
                            tables = page.find_tables()
                            for table in tables:
                                # Get the bounding box of the table
                                table_bbox = table.bbox
                                
                                # Clip the area where the table lies and remove it from the full text
                                table_text = page.get_text(clip=table_bbox)
                                full_text = full_text.replace(table_text, "")
                            
                            # Append the remaining text to the existing key in the dictionary
                            json_pdf_content['Text'] += full_text 

                # save JSON to ADLS 
                json_dumps = json.dumps(json_pdf_content, ensure_ascii=False).encode('utf8')
                dbutils.fs.put(sta_endpoint_pz_uk['01_raw'] + f"/{pz_sink_dir_json}/{clipping_id}.json", json_dumps.decode(), True)
                print(f"{pdf_file.name} has succsessfully been processed and saved to {pz_sink_dir_json}/{clipping_id}.json")
                counter = counter + 1
        
        except:
            raise Exception("Failed to write data to storage.")
else:
    print("There are no new records.")
