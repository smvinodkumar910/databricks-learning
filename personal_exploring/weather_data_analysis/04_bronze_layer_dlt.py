# Databricks notebook source
# MAGIC %run ./02_config_properties

# COMMAND ----------

import os

target_path = os.path.join('/Volumes',props.get('catalog_name'),props.get('bronze_schema'),props.get('files_volume'),
'inbound_weather_data')

filesList = os.listdir(target_path)
loc_files = []
wd_files = []
cc_files = []
for a in filesList:
  if a.startswith('LOCATION'):
    loc_files.append(os.path.join(target_path,a))
  elif a.startswith('WEATHER'):
    wd_files.append(os.path.join(target_path,a))
  elif a.startswith('CURRENT_CONDITION'):
    cc_files.append(os.path.join(target_path,a))



# COMMAND ----------

import dlt
import pyspark.sql.functions as F


@dlt.table(
    name='stg_location_dtl',
    comment='table_to_store'
)
def stg_location_dtl():
    return (spark
            .readStream
            .format('cloudFiles')
            .option('CloudFiles.format','csv')
            .option('cloudFiles.inferColumnTypes',True)
            .option("header", "true") \
            .option("rescuedDataColumn", "_rescued_data") 
            .load(os.path.join(target_path,'LOCATION_')+'*')
            .withColumn('stg_load_time',F.current_timestamp())
            .withColumn('file_name',F.input_file_name())
      )




# COMMAND ----------


@dlt.table(
    name='stg_current_condition_dtl',
    comment='table_to_store'
)
def stg_current_condition_dtl():
    return (spark
            .readStream
            .format('cloudFiles')
            .option('CloudFiles.format','csv')
            .option('cloudFiles.inferColumnTypes',True)
            .option("header", "true") \
            .option("rescuedDataColumn", "_rescued_data") 
            .load(os.path.join(target_path,'CURRENT_CONDITION')+'*')
            .withColumn('stg_load_time',F.current_timestamp())
            .withColumn('file_name',F.input_file_name())
      )

# COMMAND ----------


@dlt.table(
    name='stg_weather_dtl',
    comment='table_to_store'
)
def stg_weather_dtl():
    return (spark
            .readStream
            .format('cloudFiles')
            .option('CloudFiles.format','csv')
            .option('cloudFiles.inferColumnTypes',True)
            .option("header", "true") \
            .option("rescuedDataColumn", "_rescued_data") 
            .load(os.path.join(target_path,'WEATHER_DETAILS')+'*')
            .withColumn('stg_load_time',F.current_timestamp())
            .withColumn('file_name',F.input_file_name())
      )
