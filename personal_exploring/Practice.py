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

dbutils.fs.ls('/')


# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG weather_catalog;
# MAGIC USE SCHEMA bronze_raw;
# MAGIC DESCRIBE SCHEMA bronze_raw;

# COMMAND ----------

import pandas as pd

loc_dtl = pd.read_csv('/Volumes/weather_catalog/bronze_raw/location_dtl/LOCATION_DTL_20231128104310.csv')

display(loc_dtl)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table RAW_LOCATION_DTL AS
# MAGIC select * from read_files('/Volumes/weather_catalog/bronze_raw/location_dtl/LOCATION_DTL_20231128104310.csv',
# MAGIC    format => 'csv',
# MAGIC   header => true,
# MAGIC   mode => 'FAILFAST');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED weather_catalog.bronze_raw.raw_location_dtl;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table RAW_LOCATION_DTL_EXTERNAL LOCATION 'gs://databricks-source-bucket/weather_data/external/' AS
# MAGIC select * from read_files('/Volumes/weather_catalog/bronze_raw/location_dtl/LOCATION_DTL_20231128104310.csv',
# MAGIC    format => 'csv',
# MAGIC   header => true,
# MAGIC   mode => 'FAILFAST');
