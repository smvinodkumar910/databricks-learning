# Databricks notebook source
# MAGIC %md
# MAGIC ###1. Creating Storage Credentials
# MAGIC Step 1 : copy the service account and grant below roles on gcs bucked u desired to store delta lake tables and data etc. 
# MAGIC
# MAGIC Step 2 : Please grant the following service account email "Storage Legacy Bucket Reader" and "Storage Object Admin" roles on the GCS buckets you wish to access using this storage credential.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Creating External Location
# MAGIC
# MAGIC
# MAGIC Using the credentails created in the 1st step, create 2 external locations.
# MAGIC
# MAGIC 1. To store inbound files/archival historical files.
# MAGIC 2. To store managed delta lake tables.
# MAGIC 3. To store unmanaged/external detla lake table.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `smvinod-dbricks-files-location`
# MAGIC     URL 'gs://smvinod-dbricks-data/datasetFiles'
# MAGIC     WITH (STORAGE CREDENTIAL `smvinod-dbricks-data-credential`);
# MAGIC
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `smvinod-dbricks-managed-deltalake`
# MAGIC     URL 'gs://smvinod-dbricks-data/ManagedDeltaLake'
# MAGIC     WITH (STORAGE CREDENTIAL `smvinod-dbricks-data-credential`);
# MAGIC
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `smvinod-dbricks-unmanaged-deltalake`
# MAGIC     URL 'gs://smvinod-dbricks-data/UnmanagedDeltaLake'
# MAGIC     WITH (STORAGE CREDENTIAL `smvinod-dbricks-data-credential`);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS `weather_analysis_dev`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Schema
# MAGIC
# MAGIC 1. Create 3 schemas for Bronze, silver and gold layer respectively.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG weather_analysis_dev;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS BRONZE_LAYER;
# MAGIC CREATE SCHEMA IF NOT EXISTS SILVER_LAYER;
# MAGIC CREATE SCHEMA IF NOT EXISTS GOLD_LAYER;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Volume based on External Files location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS bronze_layer.weather_data
# MAGIC LOCATION 'gs://smvinod-dbricks-data/datasetFiles/weather_data';
