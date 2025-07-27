# Databricks notebook source
#PASSENGERS

import dlt
from pyspark.sql.functions import col, to_date

@dlt.table(name="bronze.passengers")
def passengers():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("header", "true").load("/Volumes/workspace/raw/rawvolume/passengers/")

@dlt.table(
name = "silver.silver_passengers"
)

def silver_passengers():
    df = spark.readStream.table("bronze.passengers")
    df = df.drop("_rescued_data")
    return df


dlt.create_streaming_table("gold.gold_passengers")

dlt.create_auto_cdc_flow(
    target = "gold.gold_passengers",
    source = "silver.silver_passengers",
    keys = ["passenger_id"],
    sequence_by = col("passenger_id"),
    stored_as_scd_type = 1
)