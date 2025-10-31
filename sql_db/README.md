# SQL-Script Deployment

The yaml file in this subfolder deploys all *.sql scripts into your sql-Database. 
This is only a script-deployment which assumes that the database is empty. If there are objects in the database, schemas and tables will be drop-created, tables will be truncated.

# Deployment
The deployment script takes all .sql scripts in the subfolder, merges them into one script and deploys it into the database in the environment. You need an admin user and the password and store them in the keyvault with the following secret-names.

sql-server-admin-name
sql-server-admin-password

First, the schemas are deployed, followed by the tables and data.