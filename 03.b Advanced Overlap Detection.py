# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md ## Line Aggregation
# MAGIC
# MAGIC > Instead of the point-to-point evaluation, we will instead be aggregating into lines and comparing as such.
# MAGIC
# MAGIC ---
# MAGIC __Last Updated:__ 27 NOV 2023 [Mosaic 0.3.12]

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
from pyspark.sql import functions as F
from pyspark.sql.types import *

# -- import mapping helpers
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

window_start = "2024-02-20T00:00:00.000+0000"
window_finish = "2024-02-22T23:59:00.000+0000"

cargos_indexed = (
    spark.table(cargo_table_ref)
    .filter(F.col("BaseDateTime").between(window_start, window_finish))
    )

print(f"count? {cargos_indexed.count():,}")
cargos_indexed.display()

# COMMAND ----------

# MAGIC %md ## Create Lines
# MAGIC
# MAGIC We can `groupBy` across a timewindow to give us aggregated geometries to work with.
# MAGIC
# MAGIC When we collect the various points within a timewindow, we want to construct the linestring by the order in which they were generated (timestamp).
# MAGIC We choose a buffer of a max of 200 metres in this case.
# MAGIC Since our projection is not in metres, we convert from decimal degrees. With `(0.00001 - 0.000001)` as being equal to one metre at the equator
# MAGIC Ref: http://wiki.gis.com/wiki/index.php/Decimal_degrees

# COMMAND ----------

spark.catalog.clearCache()                       # <- cache is useful for dev (avoid recompute)
lines = (
    cargos_indexed
        .repartition(sc.defaultParallelism * 20) # <- repartition is important!
        .groupBy("mmsi", F.window("BaseDateTime", "15 minutes"))
            # We link the points to their respective timestamps in the aggregation
            .agg(F.collect_list(F.struct(F.col("point_geom"), F.col("BaseDateTime"))).alias("coords"))
        # And then sort our array of points by the timestamp to form a trajectory
        .withColumn(
            "coords",
            F.expr(
                """
                array_sort(coords, (left, right) -> 
                    case 
                        when left.BaseDateTime < right.BaseDateTime then -1 
                        when left.BaseDateTime > right.BaseDateTime then 1 
                    else 0 
                end
            )"""
        ),
    )
    .withColumn("line", F.expr("st_aswkt(st_makeline(transform(coords.point_geom, p -> st_geomfromtext(p))))"))
    .cache()
)
print(f"count? {lines.count():,}")

# COMMAND ----------

# MAGIC %md _Note here that this decreases the total number of rows across which we are running our comparisons._

# COMMAND ----------

one_metre = 0.00001 - 0.000001
buffer = 200 * one_metre


def get_buffer(line, buffer=buffer):
    """Create buffer as function of number of points in linestring
    The buffer size is inversely proportional to the number of points, providing a larger buffer for slower ships.
    The intuition behind this choice is held in the way AIS positions are emitted. The faster the vessel moves the
    more positions it will emit — yielding a smoother trajectory, where slower vessels will yield far fewer positions
    and a harder to reconstruct trajectory which inherently holds more uncertainty.

    Args:
        line (geometry): linestring geometry as generated with st_makeline.

    Returns:
        double: buffer size in degrees
    """
    np = F.expr(f"st_npoints({line})")
    max_np = lines.select(F.max(np)).collect()[0][0]
    return F.lit(max_np) * F.lit(buffer) / np


cargo_movement = (
    lines
    .withColumn("buffer_r", get_buffer("line"))
    .withColumn("buffer_geom", F.expr("st_astext(st_simplify(st_buffer(line, buffer_r), 1e-3))"))
    .where("st_area(st_geogfromtext(buffer_geom)) < 1e9")
    .withColumn("ix", F.explode(F.expr("h3_tessellateaswkb(buffer_geom, 9)")))
)

cargo_movement.createOrReplaceTempView("ship_path") # <- create a temp view
spark.read.table("ship_path").display()

# COMMAND ----------

to_plot = spark.read.table("ship_path").select("buffer_geom").limit(3_000).distinct()

# COMMAND ----------

# MAGIC %md _Example buffer paths_

# COMMAND ----------

helpers.map_render(to_plot, "buffer_geom")

# COMMAND ----------

# MAGIC %md ## Find All Candidates
# MAGIC
# MAGIC We employ a join strategy using Mosaic indices as before, but this time we leverage the buffered ship paths.

# COMMAND ----------

candidate_lines_table_ref = f"{catalog_name}.{db_name}.overlap_candidates_lines"

candidates_lines = (
    cargo_movement.alias("a")
    .join(
        cargo_movement.alias("b"),
        [
            F.col("a.ix.cellid")
            == F.col("b.ix.cellid"),  # to only compare across efficient indices
            F.col("a.mmsi")
            < F.col("b.mmsi"),  # to prevent comparing candidates bidirectionally
            F.col("a.window")
            == F.col("b.window"),  # to compare across the same time window
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
        F.col("a.mmsi").alias("vessel_1"),
        F.col("b.mmsi").alias("vessel_2"),
        F.col("a.window").alias("window"),
        F.col("a.buffer_geom").alias("line_1"),
        F.col("b.buffer_geom").alias("line_2"),
        F.col("a.ix.cellid").alias("index"),
    )
    .drop_duplicates()
)

(
    candidates_lines.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(candidate_lines_table_ref)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM overlap_candidates_lines LIMIT 1

# COMMAND ----------

# MAGIC %md _We can show the most common locations for overlaps happening, as well some example ship paths during those overlaps._

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW agg_overlap AS
# MAGIC SELECT index AS ix, count(*) AS count, FIRST(line_1) AS line_1, FIRST(line_2) AS line_2
# MAGIC FROM ship2ship.overlap_candidates_lines
# MAGIC GROUP BY ix
# MAGIC ORDER BY count DESC

# COMMAND ----------

agg_overlap = (
  spark
  .table("agg_overlap")
  .orderBy(F.col("count").desc())
  .where("count < 1e2")
  )

agg_overlap.display()

helpers.map_render_h3(agg_overlap, "ix")

# COMMAND ----------

# MAGIC %md ## Filtering Out Harbours
# MAGIC In the data we see many overlaps near harbours. We can reasonably assume that these are overlaps due to being in close proximity of the harbour, not a transfer.
# MAGIC Therefore, we can filter those out below.

# COMMAND ----------

harbours_h3 = spark.read.table(f"{catalog_name}.{db_name}.harbours_h3")
candidates = spark.read.table("overlap_candidates_lines")

# COMMAND ----------

matches = (
    candidates.join(
        harbours_h3, how="leftanti", on=candidates["index"] == harbours_h3["h3"]
    )
    .groupBy("vessel_1", "vessel_2")
    .agg(F.first("line_1").alias("line_1"), F.first("line_2").alias("line_2"))
)

# COMMAND ----------

filtered_lines_table_ref = f"{catalog_name}.{db_name}.overlap_candidates_lines_filtered"

(
    matches.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(filtered_lines_table_ref)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT format_number(COUNT(1),0) FROM overlap_candidates_lines_filtered;

# COMMAND ----------

helpers.map_render(spark.table(filtered_lines_table_ref), "line_1")
