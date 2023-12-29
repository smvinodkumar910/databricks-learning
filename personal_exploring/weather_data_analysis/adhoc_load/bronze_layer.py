# Databricks notebook source
# MAGIC %sql
# MAGIC LIST '/Volumes/weather_analysis_dev/bronze_layer/weather_data/inbound_weather_data/'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/Volumes/weather_analysis_dev/bronze_layer/weather_data/inbound_weather_data/LOCATION_DTL_20231229094954.csv`;
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F

df = (spark.read.csv('/Volumes/weather_analysis_dev/bronze_layer/weather_data/inbound_weather_data/LOCATION_DTL_20231229094954.csv',header=True,inferSchema=True)
      .withColumn('stg_load_time',F.current_timestamp())
      .withColumn('file_name',F.input_file_name())
      )




# COMMAND ----------

df.write.saveAsTable('weather_analysis_dev.bronze_layer.stg_location_dtl')

# COMMAND ----------

cc_df = (spark.read.csv('/Volumes/weather_analysis_dev/bronze_layer/weather_data/inbound_weather_data/CURRENT_CONDITION_DTL_20231229094954.csv',header=True,inferSchema=True)
      .withColumn('stg_load_time',F.current_timestamp())
      .withColumn('file_name',F.input_file_name())
      )

cc_df.write.saveAsTable('weather_analysis_dev.bronze_layer.stg_current_condition_dtl')

# COMMAND ----------

wd_df = (spark.read.csv('/Volumes/weather_analysis_dev/bronze_layer/weather_data/inbound_weather_data/WEATHER_DETAILS_DTL_20231229094954.csv',header=True,inferSchema=True)
      .withColumn('stg_load_time',F.current_timestamp())
      .withColumn('file_name',F.input_file_name())
      )

wd_df.write.saveAsTable('weather_analysis_dev.bronze_layer.stg_weather_dtl')

# COMMAND ----------

import dlt

@dlt.table(
    name='stg_location_dtl',
    comment='table_to_store'
)
def stg_location_dtl():
    return (spark.read.csv('/Volumes/weather_analysis_dev/bronze_layer/weather_data/inbound_weather_data/LOCATION_DTL_20231229094954.csv',header=True,inferSchema=True)
      .withColumn('stg_load_time',F.current_timestamp())
      .withColumn('file_name',F.input_file_name())
      )
