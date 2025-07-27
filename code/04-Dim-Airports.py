# Databricks notebook source
#AIRPORTS

import dlt
from pyspark.sql.functions import col, to_date

@dlt.table(name="bronze.airports")
def airports():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("header", "true").load("/Volumes/workspace/raw/rawvolume/airports/")

@dlt.table(
name = "silver.silver_airports"
)

def silver_airports():
    df = spark.readStream.table("bronze.airports")
    df = df.drop("_rescued_data")
    return df


dlt.create_streaming_table("gold.gold_airports")

dlt.create_auto_cdc_flow(
    target = "gold.gold_airports",
    source = "silver.silver_airports",
    keys = ["airport_id"],
    sequence_by = col("airport_id"),
    stored_as_scd_type = 1
)