# Build wheels to use your own functions in Databricks

Use this directory to build your own functions and deploy them to your databricks cluster.
Use the setup file in this directory to create the wheel.

## Key-Vault
Secrets in dev-KeyVault needed for the deployment process:

- dbw-token-devops: the bearer token to access databricks
- dbw-clusterid: the cluster ID
- subscription-id: ID of the subscription
- dbw-instance: for example adb-70XXXXXXXXXX3972.1X
- dbw-workspace-name: the name of the databricks Ressource

## Deployment
Connect the release_wheel_dbx_cluster.yaml file to a new pipeline. Remember the use of \_\_init\_\_.py files in every subfolder. The deployment will take place from this folder, so every subfolder has to be called when accessing the function. 

``` from helpers import * ```