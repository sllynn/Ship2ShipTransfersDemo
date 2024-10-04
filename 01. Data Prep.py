# Databricks notebook source
# MAGIC %md ## Setup
# MAGIC
# MAGIC > Generates the table 'harbours_h3' in database 'ship2ship'.

# COMMAND ----------

# - optionally comment out to not use kepler
%pip install keplergl==0.3.2 --quiet
dbutils.library.restartPython() # <- restart python kernel

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# -- configure AQE for more compute heavy operations
spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False)

# -- import databricks + spark functions
from pyspark.sql import functions as F

# -- mapping helpers
import helpers

# --other imports
import os
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md __Configure Database__
# MAGIC
# MAGIC > Note: Adjust this to your own specified [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/admin-privileges.html#managing-unity-catalog-metastores) Schema.

# COMMAND ----------

catalog_name = "stuart"
sql(f"use catalog {catalog_name}")

db_name = "ship2ship"
sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{db_name}")
sql(f"use schema {db_name}")

volume_name = "raw"
sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{db_name}.{volume_name}")

# COMMAND ----------

# MAGIC %md __AIS Data Download: `ETL_DIR` + `ETL_DIR_FUSE`__
# MAGIC
# MAGIC > Downloading initial data into a temp location. After the Delta Tables have been created, this location can be removed. You can alter this, of course, to match your preferred location. __Note:__ this is showing DBFS for continuity outside Unity Catalog + Shared Access clusters, but you can easily modify paths to use [Volumes](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html), see more details [here](https://databrickslabs.github.io/mosaic/usage/installation.html) as available.

# COMMAND ----------

ETL_DIR = '/Volumes/stuart/ship2ship/raw'

os.environ['ETL_DIR'] = ETL_DIR
# os.environ['ETL_DIR_FUSE'] = ETL_DIR_FUSE

# dbutils.fs.mkdirs(ETL_DIR)
# print(f"...ETL_DIR: '{ETL_DIR}', ETL_DIR_FUSE: '{ETL_DIR_FUSE}' (create)")

# COMMAND ----------

schema = """
  MMSI int, 
  BaseDateTime timestamp, 
  LAT double, 
  LON double, 
  SOG double, 
  COG double, 
  Heading double, 
  VesselName string, 
  IMO string, 
  CallSign string, 
  VesselType int, 
  Status int, 
  Length int, 
  Width int, 
  Draft double, 
  Cargo int, 
  TranscieverClass string
"""

AIS_df = (
    spark.read.csv(ETL_DIR, header=True, schema=schema)
    .filter("VesselType = 70")    # <- only select cargos
    .filter("Status IS NOT NULL")
)

AIS_df.display()

# COMMAND ----------

(
  AIS_df.write
  .format("delta")
  .mode("overwrite")
  .saveAsTable(f"{catalog_name}.{db_name}.AIS")
  )

# COMMAND ----------

# MAGIC %sql select format_number(count(1), 0) as count from AIS

# COMMAND ----------

# MAGIC %md ## Harbours
# MAGIC
# MAGIC This data can be obtained from [here](https://data-usdot.opendata.arcgis.com/datasets/usdot::ports-major/about), and loaded with the code below.
# MAGIC
# MAGIC To avoid detecting overlap close to, or within harbours, in Notebook `03.b Advanced Overlap Detection` we filter out events taking place close to a harbour.
# MAGIC Various approaches are possible, including filtering out events too close to shore, and can be implemented in a similar fashion.
# MAGIC
# MAGIC In this instance we set a buffer of `10 km` around harbours to arbitrarily define an area wherein we do not expect ship-to-ship transfers to take place.
# MAGIC Since our projection is not in metres, we convert from decimal degrees. With `(0.00001 - 0.000001)` as being equal to one metre at the equator
# MAGIC Ref: http://wiki.gis.com/wiki/index.php/Decimal_degrees

# COMMAND ----------

# MAGIC %sh
# MAGIC # we download data to dbfs:// mountpoint (/dbfs)
# MAGIC cd $ETL_DIR && \
# MAGIC   wget -O harbours.geojson "https://hub.arcgis.com/api/v3/datasets/e3b6065cce144be8a13a59e03c4195fe_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1"
# MAGIC
# MAGIC ls -lh $ETL_DIR

# COMMAND ----------

one_metre = 0.00001 - 0.000001
buffer = 10 * 1000 * one_metre

major_ports = (
    spark.read.format("json")
    .option("multiline", "true")
    .load(f"{ETL_DIR}/harbours.geojson")
    .select("type", F.explode(F.col("features")).alias("feature"))
    .select(
        "type",
        F.col("feature.properties").alias("properties"),
        F.to_json(F.col("feature.geometry")).alias("json_geometry"),
    )
    .withColumn("geom", F.expr("st_aswkt(st_geomfromgeojson(json_geometry))"))
    .select(F.col("properties.PORT_NAME").alias("name"), "geom")
    .withColumn("geom", F.expr(f"st_aswkt(st_buffer(geom, {buffer}))"))
)
major_ports.limit(1).display() # <- limiting for ipynb only

# COMMAND ----------

helpers.map_render(major_ports, "geom")

# COMMAND ----------

indexed_harbours_table_ref = f"{catalog_name}.{db_name}.harbours_h3"

(
    major_ports.select("name", F.explode(F.expr("h3_tessellateaswkb(geom, 9)")).alias("mos"))
    .select("name", F.col("mos.cellid").alias("h3"))
    .write
    .mode("overwrite")
    .saveAsTable(indexed_harbours_table_ref)
)

# COMMAND ----------

harbours_h3 = spark.read.table(indexed_harbours_table_ref)
display(harbours_h3)

# COMMAND ----------

helpers.map_render_h3(harbours_h3.where("name='Port of Long Beach, CA'"), "h3")
