-- Databricks notebook source

use catalog workspace

-- COMMAND ----------

create schema if not exists bronze;

-- COMMAND ----------

create schema if not exists silver;

-- COMMAND ----------

create schema if not exists gold;

-- COMMAND ----------

create schema if not exists raw;

-- COMMAND ----------

create volume if not exists raw.rawvolume

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/airports")
-- MAGIC dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/bookings")
-- MAGIC dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/passengers")
-- MAGIC dbutils.fs.mkdirs("/Volumes/workspace/raw/rawvolume/flights")