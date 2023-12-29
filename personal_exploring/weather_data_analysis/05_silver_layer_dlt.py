# Databricks notebook source


df = spark.table('weather_analysis_dev.bronze_layer.stg_location_dtl')

# COMMAND ----------

import pyspark.sql.functions as F

df = (df
      .drop('stg_load_time','file_name')
      .withColumn('tgt_create_dt',F.current_timestamp())
      .withColumn('tgt_update_dt',F.current_timestamp())
      )

df.write.mode('overwrite').option('mergeSchema','true').saveAsTable('weather_analysis_dev.silver_layer.location_dtl')

# COMMAND ----------


cc_df = spark.table('weather_analysis_dev.bronze_layer.stg_current_condition_dtl')
cc_df.display()

cc_df = (cc_df
        .drop('file_name','stg_load_time')
        .withColumn('tgt_create_dt',F.current_timestamp())
        .withColumn('tgt_update_dt',F.current_timestamp())
         )

cc_df.write.mode('overwrite').option('mergeSchema','true').saveAsTable('weather_analysis_dev.silver_layer.current_condition_dtl')

# COMMAND ----------


wd_df = spark.table('weather_analysis_dev.bronze_layer.stg_weather_dtl')
wd_df.display()

wd_df = (wd_df
        .drop('file_name','stg_load_time')
        .withColumn('tgt_create_dt',F.current_timestamp())
        .withColumn('tgt_update_dt',F.current_timestamp())
         )

wd_df.write.mode('overwrite').option('mergeSchema','true').saveAsTable('weather_analysis_dev.silver_layer.weather_dtl')

