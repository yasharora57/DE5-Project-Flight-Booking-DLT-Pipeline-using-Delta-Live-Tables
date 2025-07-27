# Databricks notebook source
#FLIGHTS

import dlt
from pyspark.sql.functions import col, to_date

@dlt.table(name="bronze.flights")
def flights():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("header", "true").load("/Volumes/workspace/raw/rawvolume/flights/")

@dlt.table(
name = "silver.silver_flights"
)

def silver_flights():
    df = spark.readStream.table("bronze.flights")
    df = df.withColumn("flight_date", to_date(col("flight_date")))\
    .drop("_rescued_data")
    return df


dlt.create_streaming_table("gold.gold_flights")

dlt.create_auto_cdc_flow(
    target = "gold.gold_flights",
    source = "silver.silver_flights",
    keys = ["flight_id"],
    sequence_by = col("flight_id"),
    stored_as_scd_type = 1
)