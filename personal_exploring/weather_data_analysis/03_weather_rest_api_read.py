# Databricks notebook source
# MAGIC %run ./02_config_properties

# COMMAND ----------

props

# COMMAND ----------

import requests
import pandas as pd
from datetime import datetime as dt
import pytz
import os
import boto3
from io import BytesIO

city_list=['chennai','mumbai','pune','hyderabad','bengaluru','surat','kolkata','ahmedabad','visakhapatnam','madurai']


url = props.get('weather_api_url')


headers = {
	"X-RapidAPI-Key": props.get('weather_api_key'),
	"X-RapidAPI-Host": props.get('weather_api_host')
}

location=[]
weather_details=[]
current_condition=[]

for city in city_list:
    querystring = {"q":city}
    response = requests.get(url, headers=headers, params=querystring)
    output = response.json()
    loc=output.get("location")
    cc = output.get("current").get("condition")
    wd = output.get("current")

    del wd['condition']
    wd['city']=loc.get("name")

    cc['weather_time'] = wd.get("last_updated")
    cc['city'] = loc.get("name")
    
    location.append(loc)
    current_condition.append(cc)
    weather_details.append(wd)

# COMMAND ----------

loc_df = pd.DataFrame(location)
cc_df = pd.DataFrame(current_condition)
wd_df = pd.DataFrame(weather_details)

# COMMAND ----------

import os

utc_time = dt.now(pytz.timezone('utc'))

target_path = os.path.join('/Volumes',props.get('catalog_name'),props.get('bronze_schema'),props.get('files_volume'),
'inbound_weather_data')


if not os.path.exists(target_path):
    os.makedirs(target_path)

file_path = utc_time.strftime('%Y%m%d%H%M%S')


loc_df.to_csv(os.path.join(target_path,"LOCATION_DTL_"+file_path+".csv"),index=None )
cc_df.to_csv(os.path.join(target_path,"CURRENT_CONDITION_DTL_"+file_path+".csv"),index=None )
wd_df.to_csv(os.path.join(target_path,"WEATHER_DETAILS_DTL_"+file_path+".csv"),index=None )



# COMMAND ----------


spark.conf.set('inbound_path',target_path)
