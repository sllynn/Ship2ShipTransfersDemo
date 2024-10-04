# Databricks notebook source
# MAGIC %md ## Setup
# MAGIC
# MAGIC ---
# MAGIC __Last Updated:__ 27 NOV 2023 [Mosaic 0.3.12]

# COMMAND ----------

# - optionally comment out to not use kepler
%pip install keplergl==0.3.2 --quiet
dbutils.library.restartPython() # <- restart python kernel

# COMMAND ----------

# -- configure AQE for more compute heavy operations
#  - choose option-1 or option-2 below, essential for REPARTITION!
# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", False) # <- option-1: turn off completely for full control
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False) # <- option-2: just tweak partition management
spark.conf.set("spark.sql.shuffle.partitions", 1_024)                  # <-- default is 200

# -- import databricks + spark functions
from pyspark.databricks.sql import functions as dbf
from pyspark.sql import functions as F

# -- mapping helpers
import helpers

# --other imports
import warnings

warnings.simplefilter("ignore")

# COMMAND ----------

# MAGIC %md __Configure Database__
# MAGIC
# MAGIC > Adjust this to settings from the Data Prep notebook.

# COMMAND ----------

catalog_name = "stuart"
db_name = "ship2ship"

sql(f"use catalog {catalog_name}")
sql(f"use schema {db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We begin with loading from a table. Here we use captured `AIS` data.
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC - `MMSI`: unique 9-digit identification code of the ship - numeric
# MAGIC - `VesselName`: name of the ship - string
# MAGIC - `CallSign`: unique callsign of the ship - string
# MAGIC - `BaseDateTime`: timestamp of the AIS message - datetime
# MAGIC - `LAT`: latitude of the ship (in degree: [-90 ; 90], negative value represents South, 91 indicates ‘not available’) - numeric
# MAGIC - `LON`: longitude of the ship (in degree: [-180 ; 180], negative value represents West, 181 indicates ‘not available’) - numeric
# MAGIC - `SOG`: speed over ground, in knots - numeric
# MAGIC - `Status`: status of the ship - string

# COMMAND ----------

raw_table_ref = f"{catalog_name}.{db_name}.AIS"

cargos = spark.read.table(raw_table_ref)
cargos.display()

# COMMAND ----------

# MAGIC %md ## AIS Data Indexing
# MAGIC
# MAGIC > To facilitate downstream analytics it is also possible to create a quick point index leveraging a chosen H3 resolution.
# MAGIC In this case, resolution `9` has an edge length of ~174 metres.

# COMMAND ----------

cargos_indexed = (
    cargos
    .withColumn("point_geom", F.expr("st_astext(st_point(LON, LAT))"))
    .withColumn("ix", dbf.h3_pointash3("point_geom", F.lit(9)))
    .withColumn("sog_kmph", F.round(F.col("sog") * 1.852, 2))
)
cargos_indexed.display()

# COMMAND ----------

# MAGIC %md _We will write the treated output to a new table._

# COMMAND ----------

indexed_cargo_table_ref = f"{catalog_name}.{db_name}.cargos_indexed"

(
    cargos_indexed
    .write.mode("overwrite")
    .saveAsTable(indexed_cargo_table_ref)
)

# COMMAND ----------

# MAGIC %md _We will optimise our table to colocate data and make querying faster._
# MAGIC
# MAGIC > This is showing [ZORDER](https://docs.databricks.com/en/delta/data-skipping.html); for newer runtimes (DBR 13.3 LTS+), can also consider [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html).

# COMMAND ----------

spark.sql(
    f"""OPTIMIZE
    {indexed_cargo_table_ref}
    ZORDER BY
    (ix, BaseDateTime)"""
).display()

# COMMAND ----------

# MAGIC %md ## Visualisation
# MAGIC And we can perform a quick visual inspection of the indexed AIS data.

# COMMAND ----------

cargos_indexed = (
  spark
  .table(indexed_cargo_table_ref)
  .where("mmsi = 477176800")
  .orderBy("BaseDateTime")
  .select(
    "ix", "mmsi", "VesselName",
    F.col("BaseDateTime").cast("string").alias("ts")
    ).limit(100_000)
  )

cargos_indexed.display()

helpers.map_render_h3(cargos_indexed, "ix")
