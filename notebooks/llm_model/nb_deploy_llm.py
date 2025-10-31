# Databricks notebook source
# MAGIC %pip uninstall -y mlflow mlflow-skinny
# MAGIC %pip install -U -qqqq pydantic==2.9.2 databricks-agents mlflow mlflow-skinny databricks-vectorsearch databricks-sdk langchain==0.3.3 langchain_core==0.3.12 langchain_community==0.3.2 typing-extensions databricks-sql-connector[sqlalchemy]==3.7.1 langchain-openai==0.2.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../common/nb_init

# COMMAND ----------

import os
import time

from databricks import agents

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.vectorsearch import EndpointStatusState, EndpointType
from databricks.sdk.service.serving import EndpointCoreConfigInput, EndpointStateReady, EndpointStateConfigUpdate
from databricks.sdk.errors import ResourceDoesNotExist, NotFound, PermissionDenied

from databricks.vector_search.client import VectorSearchClient

from pyspark.sql import SparkSession

import mlflow

# COMMAND ----------

w = WorkspaceClient()

UC_CATALOG = get_secret('dbw-pz-catalog')
UC_SCHEMA = '04_ai'
UC_MODEL_NAME = UC_CATALOG + '.' + UC_SCHEMA + '.uk_chatbot'

sql_warehouse_id = get_secret("sql-warehouse-id")

# COMMAND ----------

chain_config = {
    "llm_model_serving_endpoint_name": "databricks-meta-llama-3-1-70b-instruct", # TODO: remove
    "host": SparkSession.getActiveSession().conf.get("spark.databricks.workspaceUrl", None),
    "warehouse_id": sql_warehouse_id,
    "dbw-pz-catalog": UC_CATALOG,
}

input_example = {"messages": [ {"role": "user", "content": "Welche facebook posts waren 2023 am besten?"}]}

# COMMAND ----------

with mlflow.start_run(run_name="uk-chatbot"):
    logged_chain_info = mlflow.langchain.log_model(
        lc_model = os.path.join(
            os.getcwd(),
            f"./react_llm",
        ),
        model_config = chain_config,
        artifact_path = "chain",
        input_example = input_example,
        extra_pip_requirements = [
            'pydantic==2.9.2',
            'langchain_community==0.3.2',
            'databricks-sql-connector[sqlalchemy]==3.7.1',
            'pyspark',
            'langchain-openai==0.2.2',
        ],
    )

# chain = mlflow.langchain.load_model(logged_chain_info.model_uri)
# chain.invoke(input_example)

# COMMAND ----------

# Use Unity Catalog to log the chain
mlflow.set_registry_uri('databricks-uc')

# Register the chain to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_chain_info.model_uri, name=UC_MODEL_NAME)

# Deploy to enable the Review APP and create an API endpoint
deployment_info = agents.deploy(model_name=UC_MODEL_NAME, model_version=uc_registered_model_info.version, environment_vars={
    "DATABRICKS_TOKEN": f'{{{{secrets/{secretScope}/service-principal-pz-uk-icc-rag-app-token}}}}',
    "DATABRICKS_HOST": 'https://' + chain_config['host'],
    "AZURE_OPENAI_API_KEY_PROD": f'{{{{secrets/{secretScope}/azure-openai-api-key}}}}',
})

# Wait for the Review App to be ready
print("\nWaiting for endpoint to deploy.  This can take 10 - 20 minutes.", end="")
while w.serving_endpoints.get(deployment_info.endpoint_name).state.ready == EndpointStateReady.NOT_READY or w.serving_endpoints.get(deployment_info.endpoint_name).state.config_update == EndpointStateConfigUpdate.IN_PROGRESS:
    print(".", end="")
    time.sleep(30)

# COMMAND ----------

deployment_info

# COMMAND ----------


