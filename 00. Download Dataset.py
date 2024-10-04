# Databricks notebook source
import os
import requests
import shutil
from datetime import datetime, timedelta
from zipfile import ZipFile

# COMMAND ----------

# MAGIC %sh mkdir -p /tmp/ais/zipped

# COMMAND ----------

origin = datetime(year=2024, month=2, day=1)
days_in_period = 28
for i in range(days_in_period):
  current = origin + timedelta(days=i)
  filename = f"AIS_{current.year}_{current.month:02d}_{current.day:02d}.zip"
  print(filename)
  local_uri = f"/tmp/ais/zipped/{filename}"
  remote_url = f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{current.year}/{filename}"
  with requests.get(remote_url, stream=True) as r:
      with open(local_uri, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

# COMMAND ----------

# write into a UC Volume
zip_path = "/tmp/ais/zipped"
output_path = "/Volumes/stuart/ship2ship/raw"

# COMMAND ----------

for file_name in os.listdir(zip_path):
  print(f"+ Extracting file: {file_name}")
  with ZipFile(f"{zip_path}/{file_name}") as zip_file:
    zip_file.extractall(path=output_path)
