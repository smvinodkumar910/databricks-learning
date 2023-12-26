# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating External Location

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `weather_data_external_location`
# MAGIC     URL 'gs://databricks-source-bucket/weather_data'
# MAGIC     WITH (STORAGE CREDENTIAL `databricks-gcs-bucket`);
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS `weather_catalog` MANAGED LOCATION 'gs://databricks-source-bucket/weather_data/';
# MAGIC

# COMMAND ----------

dbutils.fs.put("/FileStore/my-stuff/my-file.txt", "This is the actual text that will be saved to disk. Like a 'Hello world!' example")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS `weather_data` MANAGED LOCATION 'gs://databricks-8634720421432193/weather_data/';

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG weather_catalog;
# MAGIC USE SCHEMA bronze_raw;
# MAGIC DESCRIBE SCHEMA bronze_raw;

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

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
