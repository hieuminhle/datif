# Databricks notebook source
# MAGIC %md
# MAGIC Vector Search endpoint

# COMMAND ----------

# MAGIC %pip uninstall -y mlflow mlflow-skinny
# MAGIC %pip install -U -qqqq databricks-agents mlflow mlflow-skinny databricks-vectorsearch databricks-sdk langchain==0.2.11 langchain_core==0.2.23 langchain_community==0.2.10 

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
from databricks.sdk.core import DatabricksError
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointStatusState, EndpointType
from databricks.sdk.service.serving import EndpointCoreConfigInput, EndpointStateReady
from databricks.sdk.errors import ResourceDoesNotExist, NotFound, PermissionDenied

VECTOR_SEARCH_ENDPOINT = "vector-search-endpoint"

w = WorkspaceClient()

# Create the Vector Search endpoint if it does not exist
vector_search_endpoints = w.vector_search_endpoints.list_endpoints()
if sum([VECTOR_SEARCH_ENDPOINT == ve.name for ve in vector_search_endpoints]) == 0:
    print(f"Please wait, creating Vector Search endpoint `{VECTOR_SEARCH_ENDPOINT}`.  This can take up to 20 minutes...")
    w.vector_search_endpoints.create_endpoint_and_wait(VECTOR_SEARCH_ENDPOINT, endpoint_type=EndpointType.STANDARD)

# Make sure vector search endpoint is online and ready.
w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(VECTOR_SEARCH_ENDPOINT)

print(f"PASS: Vector Search endpoint `{VECTOR_SEARCH_ENDPOINT}` exists")

# COMMAND ----------

# MAGIC %md
# MAGIC Create the Vector Search Index

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from databricks.vector_search.client import VectorSearchClient

# UC locations to store the chunked documents & index
UC_CATALOG = "datif_pz_uk_dev"
UC_SCHEMA = "03_served"
UC_TABLE = "consolidated_socials"

CHUNKS_VECTOR_INDEX = UC_CATALOG+'.'+UC_SCHEMA+'.'+UC_TABLE+"_vector"

# Vector Search client
vsc = VectorSearchClient(disable_notice=True)

index = vsc.create_delta_sync_index_and_wait(
    endpoint_name=VECTOR_SEARCH_ENDPOINT,
    index_name=CHUNKS_VECTOR_INDEX,
    primary_key="social_post_id",  
    source_table_name="datif_pz_uk_dev.03_served.consolidated_socials",
    pipeline_type="TRIGGERED",
    embedding_source_column="post_text",
    embedding_model_endpoint_name="openai-embedding-endpoint ",
)
