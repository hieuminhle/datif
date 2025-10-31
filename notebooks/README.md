#Databricks-Notebooks
This directory should be used to link with Azure Databricks Repos. described in the main README.md.

You can create subfolder to organize your notebooks. 

#Deployment
To start the deployment, link the deploy_notebooks_to_workspace.yaml file to a new pipeline. 

The deployment pipeline will fetch all notebooks from this top-folder and move them into a folder defined within the variable "zone" in the vars.yaml file in the databricks workspace while maintaining the subfolder hierarchy. The pipeline will be triggered by all changes in the notebooks subfolder, excluding the markdown and the yaml file. 
For merging changes to dev, merge your branch into the dev branch, test for test and main for changes in the production environment.

After forking the repository you might delete the test.py file. The yaml file should remain on this level.

#Key Vault 
You need the following secrets in your KeyVault for a working deployment pipeline

- dbw-workspace-url: for example https://adb-XXXXXXXX926239XX.1X.azuredatabricks.net

- dbw-token-devops: the bearer token to access Databricks

#Best practices
We recommend the use of an init notebook to initiate your environment. Use this Notebook to create an endpoint to your storage on the fly instead of mounting it. All credentials should be stored in secrets. After initiating your environment you can execute the init notebook from every other notebook to build the connection.

```
secretScope = "keyvault"

storage = dbutils.secrets.get(scope=secretScope,key="storage-datalake-name")

clientId = dbutils.secrets.get(scope=secretScope,key="service-principal-client-id")
clientSecret = dbutils.secrets.get(scope=secretScope,key="service-principal-client-secret")
tenantId = dbutils.secrets.get(scope=secretScope,key="tenant-id")

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", clientId)
spark.conf.set("fs.azure.account.oauth2.client.secret", clientSecret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/"+tenantId+"/oauth2/token")

sta_endpointh = 'abfss://ctifh@' + storage + '.dfs.core.windows.net'
```

After initiating your environment you have to define the connection to your storage. \'abfss\' marks the driver to the datalake, next you have to define your container in the datalake, ctifh in our example, @ + storage is the name of your storage, stored as a secret in the KeyVault. \'.dfs.core.windows.net\' is the standard endpoint for the datalake.

You can call your init notebook from every other notebook with a %run statement