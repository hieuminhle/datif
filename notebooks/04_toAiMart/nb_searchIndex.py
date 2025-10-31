# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch==0.49
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../common/nb_init

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Index Table

# COMMAND ----------

target_schema_name = '04_ai'
target_path = 'search_index'

# COMMAND ----------

# DBTITLE 1,Configure Data Sources
"""
configure the tables that included in the search index.
   - "table_name": the name of the table to be indexed. catalog can be omitted, standard catalog set in nb_init ist used.
   - "id_column": the name of the primary key column
   - "channel": the name of the datasource. this can be used to figure which chunk came from which channel, i.e., facebook, instagram, print_media, ...
   - "chunk": the name of the column that contains the text to be indexed
"""


relevant_tables = [
    {
        'table_name': '03_transformed.facebook_organic_total',
        'id_column': 'PostID',
        'channel': 'Facebook',
        'chunk': 'PostMessage',
        'date': 'CreatedDate',
        'url': 'PostURL',
    },
    {
        'table_name': '03_transformed.instagram_organic_total',
        'id_column': 'PostID',
        'channel': 'Instagram',
        'chunk': 'PostMessage',
        'date': 'CreatedDate',
        'url': 'PostURL',
    },
    {
        'table_name': '03_transformed.linkedin_organic_total', #
        'id_column': 'PostID',
        'channel': 'LinkedIn',
        'chunk': 'PostContent',
        'date': 'CreatedDate',
        'url': 'PostURL',
    },
    {
        'table_name': '03_transformed.x_organic_total', 
        'id_column': 'PostID',
        'channel': 'X',
        'chunk': 'PostMessage',
        'date': 'CreatedDate',
        'url': 'PostURL',
    },
    {
        'table_name': '03_transformed.youtube_organic_post_total', 
        'id_column': 'VideoID',
        'channel': 'YouTube',
        'chunk': 'VideoDescription',
        'date': 'CreatedDate',
        'url': 'VideoURL',
    },
    {
        'table_name': '03_transformed.ga4_eco_journal_users_sessions_total_view', 
        'id_column': 'Page_Path_ECO',
        'channel': 'ECO-Journal',
        'chunk': 'Abstract',
        'date': 'Created_Date',
        'url': 'URL',
    },
    # TODO: include genios print media, if available
    # TODO: include genios online media, if available 
    #{
    #    'table_name': '03_transformed.youtube_organic_post_total', 
    #    'id_column': 'VideoID',
    #    'channel': 'YouTube',
    #    'chunk': 'VideoDescription',
    #    'date': 'CreatedDate',
    #    'url': '',
    #},
]

# COMMAND ----------

# DBTITLE 1,Combine all relevant tables
schema = T.StructType([
    T.StructField("ID", T.StringType(), True),
    T.StructField("Channel", T.StringType(), True),
    T.StructField("Chunk", T.StringType(), True),
    T.StructField("Date", T.StringType(), True),
    T.StructField("URL", T.StringType(), True),
])
df_consolidated = spark.createDataFrame([], schema=schema)

for t in relevant_tables:
    df = spark.read.table(f"datif_pz_uk_{env}.{t['table_name']}").select(
        F.col(t['id_column']).alias("ID"),
        F.col(t['date']).alias("Date"),
        F.lit(t['channel']).alias("Channel"),
        F.col(t['chunk']).alias("Chunk"),
        F.col(t['url']).alias("URL"),
    ).where(F.col("Date") >= "2025-01-01")
    
    df_consolidated = df_consolidated.unionByName(df)

df_consolidated.display()

# COMMAND ----------

# DBTITLE 1,Write Table
fn_overwrite_table(df_source=df_consolidated, target_schema_name=target_schema_name, target_table_name="search_index_table", target_path=target_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `04_ai`.search_index_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true) -- is necessary to create an search index!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Index

# COMMAND ----------

# DBTITLE 1,Create Search Index
from databricks.vector_search.client import VectorSearchClient

c = VectorSearchClient()

SEARCH_INDEX_TABLE = f"{UC_CATALOG}.{target_schema_name}.search_index_table"
SEARCH_VECTOR_INDEX = f"{UC_CATALOG}.{target_schema_name}.search_vector_index"
ENDPOINT_NAME = "uk-search-endpoint"

# check if endpoint exists, create and wait for completion if not
try:
    c.get_endpoint(ENDPOINT_NAME)
    print("Endpoint exists.")
except Exception as e:
    if "NOT_FOUND" in str(e):
        print("Endpoint does not exist. Creating Endpoint...")
        c.create_endpoint_and_wait(
            name=ENDPOINT_NAME,
            endpoint_type="STANDARD",
        )
        print("Endpoint created.")

# if index exists, sync
# if index does not exist, create
try:
    index = c.get_index(ENDPOINT_NAME, SEARCH_VECTOR_INDEX)
    print("Index exists. Starting Sync... This can take a while and finishes in the background.")
    index.sync()
except NameError as e: # catching both exceptions, because both occured during development
    print("Index does not exist. Creating Index...")
    index = c.create_delta_sync_index_and_wait(
        endpoint_name=ENDPOINT_NAME,
        index_name=SEARCH_VECTOR_INDEX,
        primary_key="ID",  
        source_table_name=SEARCH_INDEX_TABLE,
        # pipeline_type="CONTINUOUS",
        pipeline_type="TRIGGERED",
        embedding_source_column="Chunk",
        embedding_model_endpoint_name="openai-embedding-endpoint ",
    )
    print("Index created.")
except Exception as e: # catching both exceptions, because both occured during development
    if "RESOURCE_DOES_NOT_EXIST" in str(e):
        print("Index does not exist. Creating Index...")
        index = c.create_delta_sync_index_and_wait(
            endpoint_name=ENDPOINT_NAME,
            index_name=SEARCH_VECTOR_INDEX,
            primary_key="ID",  
            source_table_name=SEARCH_INDEX_TABLE,
            # pipeline_type="CONTINUOUS",
            pipeline_type="TRIGGERED",
            embedding_source_column="Chunk",
            embedding_model_endpoint_name="openai-embedding-endpoint ",
        )
        print("Index created.")
