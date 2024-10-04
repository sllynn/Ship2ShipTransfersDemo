# Databricks notebook source
# MAGIC %md # Overlap Detection
# MAGIC
# MAGIC > We now try to detect potentially overlapping pings using a buffer on a particular day.

# COMMAND ----------

# MAGIC %md ## Setup

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
from pyspark.sql.types import *

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

cargo_table_ref = f"{catalog_name}.{db_name}.cargos_indexed"

window_start = "2024-02-21T00:00:00.000+0000"
window_finish = "2024-02-21T23:59:00.000+0000"

cargos_indexed = (
    spark.table(cargo_table_ref)
    .filter(F.col("BaseDateTime").between(window_start, window_finish))
    )

print(f"count? {cargos_indexed.count():,}")
cargos_indexed.display()

# COMMAND ----------

# MAGIC %md ## Buffering
# MAGIC
# MAGIC <p/>
# MAGIC
# MAGIC 1. Convert the point into a polygon by buffering it with a certain area to turn this into a circle.
# MAGIC 2. Index the polygon to leverage more performant querying.
# MAGIC
# MAGIC > Since our projection is not in metres, we convert from decimal degrees, with `(0.00001 - 0.000001)` as being equal to one metre at the equator. Here we choose an buffer of roughly 100 metres, ref http://wiki.gis.com/wiki/index.php/Decimal_degrees.

# COMMAND ----------

buffered_cargo_table_ref = f"{catalog_name}.{db_name}.cargos_buffered"

one_metre = 0.00001 - 0.000001
buffer = 100 * one_metre

(
    cargos_indexed
    .repartition(sc.defaultParallelism * 20) # <- repartition is important!
    .withColumn("buffer_geom", F.expr(f"st_aswkt(st_buffer(point_geom, {buffer}))"))
    .withColumn("ix", F.explode(F.expr("h3_tessellateaswkb(buffer_geom, 9)")))
    .write.mode("overwrite")
    .saveAsTable(buffered_cargo_table_ref)
)

# COMMAND ----------

# MAGIC %md _We will optimise our table to colocate data and make querying faster._
# MAGIC
# MAGIC > This is showing [ZORDER](https://docs.databricks.com/en/delta/data-skipping.html); for newer runtimes (DBR 13.3 LTS+), can also consider [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html).

# COMMAND ----------

spark.sql(
    f"""OPTIMIZE
    {buffered_cargo_table_ref}
    ZORDER BY
    (ix.cellid, BaseDateTime)"""
).display()

# COMMAND ----------

spark.table(buffered_cargo_table_ref).display()

# COMMAND ----------

# MAGIC %md ## Implement Algorithm

# COMMAND ----------

def time_window(sog1, sog2, heading1, heading2, radius):
    """Create dynamic time window based on speed, buffer radius and heading.

    Args:
        sog1 (double): vessel 1's speed over ground, in knots
        sog2 (double): vessel 2's speed over ground, in knots
        heading1 (double): vessel 1's heading angle in degrees
        heading2 (double): vessel 2's heading angle in degrees
        radius (double): buffer radius in degrees

    Returns:
        double: dynamic time window in seconds based on the speed and radius
    """
    v_x1 = F.col(sog1) * F.cos(F.col(heading1))
    v_y1 = F.col(sog1) * F.sin(F.col(heading1))
    v_x2 = F.col(sog2) * F.cos(F.col(heading2))
    v_y2 = F.col(sog2) * F.sin(F.col(heading2))

    # compute relative vectors speed based x and y partial speeds
    v_relative = F.sqrt((v_x1 + v_x2) * (v_x1 + v_x2) + (v_y1 + v_y2) * (v_y1 + v_y2))
    # convert to m/s and determine ratio between speed and radius
    return v_relative * F.lit(1000) / F.lit(radius) / F.lit(3600)


candidates = (
    spark.table(buffered_cargo_table_ref)
    .alias("a")
    .join(
        spark.table(buffered_cargo_table_ref).alias("b"),
        [
            # to only compare across efficient indices
            F.col("a.ix.cellid") == F.col("b.ix.cellid"),
            # to prevent comparing candidates bidirectionally
            F.col("a.mmsi") < F.col("b.mmsi"),
            F.abs(F.timestamp_diff("second", "a.BaseDateTime", "b.BaseDateTime"))
            < time_window("a.sog_kmph", "b.sog_kmph", "a.heading", "b.heading", buffer),
        ],
    )
    .where(
        (
            F.col("a.ix.core") | F.col("b.ix.core")
        )  # if either candidate fully covers an index, no further comparison is needed
        | F.expr("st_intersects(a.ix.chip, b.ix.chip)")
        # limit geospatial querying to cases where indices alone cannot give certainty
    )
    .select(
        F.col("a.vesselName").alias("vessel_1"),
        F.col("b.vesselName").alias("vessel_2"),
        F.col("a.BaseDateTime").alias("timestamp_1"),
        F.col("b.BaseDateTime").alias("timestamp_2"),
        F.col("a.ix.cellid").alias("ix"),
    )
)

# COMMAND ----------

overlap_candidates_table_ref = f"{catalog_name}.{db_name}.overlap_candidates"
(
  candidates.write
  .mode("overwrite")
  .saveAsTable(overlap_candidates_table_ref)
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM overlap_candidates limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW agg_overlap AS
# MAGIC SELECT ix, count(*) AS count
# MAGIC FROM overlap_candidates
# MAGIC GROUP BY ix, vessel_1, vessel_2
# MAGIC ORDER BY count DESC;
# MAGIC
# MAGIC SELECT * FROM agg_overlap LIMIT 10;

# COMMAND ----------

# MAGIC %md ## Plot Common Overlaps

# COMMAND ----------

# MAGIC %md _Uncomment the following within databricks for actual results._

# COMMAND ----------

agg_overlap = (
  spark
  .table("agg_overlap")
  .orderBy(F.col("count").desc())
  .limit(100)
  )

agg_overlap.display()

helpers.map_render_h3(agg_overlap, "ix")
