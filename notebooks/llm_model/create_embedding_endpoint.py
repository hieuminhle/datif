# Databricks notebook source
# MAGIC %md
# MAGIC Step 1: Install MLflow with external models support
# MAGIC

# COMMAND ----------

# MAGIC %pip install mlflow[genai]==2.9.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Create and manage an external model endpoint

# COMMAND ----------

import mlflow.deployments

client = mlflow.deployments.get_deploy_client("databricks")

# COMMAND ----------

client.create_endpoint(
    name="openai-embedding-endpoint",
    config={
        "served_entities": [
          {
            "name": "openai-embedding",
            "external_model": {
                "name": "text-embedding-ada-002",
                "provider": "openai",
                "task": "llm/v1/embeddings",
                "openai_config": {
                    "openai_api_type": "azure",
                    "openai_api_key": "{{secrets/keyVault/azure-openai-api-key}}",
                    "openai_api_base": "https://api.competence-cente-cc-genai-prod.enbw-az.cloud/",
                    "openai_deployment_name": "text-embedding-ada-002",
                    "openai_api_version": "2024-06-01"
                },
            },
          }
        ],
    },
)

# COMMAND ----------

client.create_endpoint(
    name="openai-gpt-4o",
    config={
        "served_entities": [
          {
            "name": "openai-gpt-4o",
            "external_model": {
                "name": "gpt-4o",
                "provider": "openai",
                "task": "llm/v1/chat",
                "openai_config": {
                    "openai_api_type": "azure",
                    "openai_api_key": "{{secrets/keyVault/azure-openai-api-key}}",
                    "openai_api_base": "https://api.competence-cente-cc-genai-prod.enbw-az.cloud/",
                    "openai_deployment_name": "gpt-4o",
                    "openai_api_version": "2024-06-01"
                },
            },
          }
        ],
    },
)
