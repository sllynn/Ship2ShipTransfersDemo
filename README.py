# Databricks notebook source
# MAGIC %md
# MAGIC # Identifying possible ship-to-ship transfers from AIS data
# MAGIC
# MAGIC <img src="./assets/heatmap.png"/>
# MAGIC
# MAGIC ## Background
# MAGIC AIS transponders transmit data against a vessel's unique identifier such as its current position and heading. This data has applications, for example, in collision avoidance and scheduling of port operations. A government agency may also want to collect and process this data in order to identify suspicious behaviours. These could be short, unscheduled landings or rendezvous with other vessels at sea, a possible indicator of ship-to-ship transfers of contraband.
# MAGIC
# MAGIC ## Dataset
# MAGIC The dataset downloaded and used in this demo is published by the US Office of Coastal Management. The data covers an area of coastal waters around the USA over a period of a month. One days' worth of observations equates to around ten million points containing lon/lat pairs, timestamps and vessel identifiers. Observations are not evenly spaced in time or spatial dimensions.
# MAGIC
# MAGIC ## Objective
# MAGIC Identify vessels whose paths have crossed within some reasonable time window and report the vessel identifiers and point or points at which they likely intersected.
# MAGIC
# MAGIC ## Approach
# MAGIC ### Geometry types
# MAGIC Geometry types in Spark Dataframes as shown here are currently a private preview feature. This will allow us to create a point geometry for each vessel location, compute their courses as linestrings generate polygons relating to major ports.
# MAGIC
# MAGIC In order to enable these functions, you will need to set the following Spark conf on your cluster (or at the top of each notebook using `spark.conf.set()`):
# MAGIC `spark.databricks.geo.st.enabled true`
# MAGIC
# MAGIC ### Spatial operations
# MAGIC In order to identify whether the paths of two vessels have intersected, we will use the spatial SQL functions available as part of the same private preview, including the ability to 'mosaic' geometries into tessellating 'chips' using the H3 global grid indexing system.
# MAGIC
# MAGIC ### Spatial indexing
# MAGIC Because we are performing a pairwise comparison of the paths of many thousands of vessels, we would like to employ some form of spatial indexing to partition the problem. For this we will use Uber's H3 library, which provides a hierarchical, hexagonal grid index system.
# MAGIC
# MAGIC ### Visualisation
# MAGIC The `kepler.gl` library provides an easy to use interface for visualisation of geospatial data in Python.
