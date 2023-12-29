# Databricks notebook source
# MAGIC %run ./02_config_properties

# COMMAND ----------

import os
import shutil

source_path = os.path.join('/Volumes',props.get('catalog_name'),props.get('bronze_schema'),props.get('files_volume'),
'inbound_weather_data')

target_path = os.path.join('/Volumes',props.get('catalog_name'),props.get('bronze_schema'),props.get('files_volume'),
'archival_weather_data')


if not os.path.exists(target_path):
    os.makedirs(target_path)

# COMMAND ----------

files = os.listdir(source_path)

for a in files:
    shutil.move(os.path.join(source_path,a),os.path.join(target_path,a))

