# Databricks notebook source
#BOOKINGS

import dlt
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType

@dlt.table(
    name="bronze.bookings",
    comment="Raw bookings data ingested daily in Bronze",
)
def bookings():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/workspace/raw/rawvolume/bookings/")
    )

@dlt.table(
    name = "silver.silver_bookings"
)

def silver_bookings():
    df = spark.readStream.table("bronze.bookings")
    df = df.withColumn("amount",col("amount").cast(DoubleType()))\
    .withColumn("booking_date", to_date(col("booking_date")))\
    .drop("_rescued_data")
    return df

rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

@dlt.table (
    name = "gold.gold_bookings"
)

@dlt.expect_all_or_drop(rules)
def gold_bookings():
    df = spark.readStream.table("silver.silver_bookings")
    return df