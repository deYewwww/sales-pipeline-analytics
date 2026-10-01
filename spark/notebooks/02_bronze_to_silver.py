# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Observe the raw_payload from bronze table
# MAGIC SELECT 
# MAGIC     raw_payload 
# MAGIC FROM sales_pipeline_analytics.bronze.raw_deal_events 
# MAGIC LIMIT 3;

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, StringType, DoubleType)

# Schema for incoming JSON
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("deal_id", StringType()),
    StructField("event_type", StringType()),
    StructField("owner", StringType()),
    StructField("deal_name", StringType()),
    StructField("deal_value_rm", DoubleType()),
    StructField("old_stage", StringType()),
    StructField("new_stage", StringType()),
    StructField("timestamp", StringType()),
    StructField("metadata", StructType([
        StructField("source", StringType()),
        StructField("vertical", StringType())
    ]))
])

# COMMAND ----------

'''
Build Silver Dataframe 
'''
from pyspark.sql.functions import (col, from_json, to_timestamp, current_timestamp)
from pyspark.sql.types import DecimalType

# 1.Read bronze table 
bronze_df = spark.read.table("sales_pipeline_analytics.bronze.raw_deal_events")

# 2. Parse JSON 
parsed_bronze_df = bronze_df.withColumn("parsed", from_json(col("raw_payload"), event_schema))

# 3. Extracting fields
silver_df = (parsed_bronze_df
             .select(
                 col("parsed.event_id"),
                 col("parsed.deal_id"),
                 col("parsed.event_type"),
                 col("parsed.owner"),
                 col("parsed.deal_name"),
                 col("parsed.deal_value_rm").cast(DecimalType(10,2)).alias("deal_value_rm"),
                 col("parsed.old_stage"),
                 col("parsed.new_stage"),
                 to_timestamp(col("parsed.timestamp")).alias("event_timestamp"),
                 col("parsed.metadata.source").alias("metadata_source"),
                 col("parsed.metadata.vertical").alias("metadata_vertical"),
                 col("_kafka_partition").alias("_kafka_partition"),
                 col("_kafka_offset").alias("_kafka_offset"),
                 col("_kafka_timestamp").alias("_kafka_timestamp"),
                 current_timestamp().alias("_ingested_at")
            ))

    
# Verify schema
silver_df.printSchema()
# showing deal_id with full (show() will cutoff strings longer than 20)
silver_df.show(5, truncate=False)




# COMMAND ----------

'''
Build a quality flag column
'''
from pyspark.sql.functions import(col, when, array, array_compact, lit)

quality_flags = array(
    when(col("deal_value_rm").isNull(), lit("missing_deal_value")),
    when(col("new_stage").isNull(), lit("missing_stage")),
    when(col("event_timestamp").isNull(), lit("missing_timestamp")),
    when(
        (col("event_type") == "stage_changed") & (col("old_stage") == col("new_stage")), 
        lit("stage_unchanged")
    )

)

# Add quality flags to silver dataframe
silver_df = silver_df.withColumn("_quality_flags", array_compact(quality_flags))
silver_df.show(5, truncate=False)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create the silver schema
# MAGIC CREATE SCHEMA IF NOT EXISTS sales_pipeline_analytics.silver;
# MAGIC
# MAGIC

# COMMAND ----------

'''
Write to silver table
'''

spark.sql("USE CATALOG sales_pipeline_analytics")

# Write to silver table
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("sales_pipeline_analytics.silver.deal_events_cleaned")
)



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 1. Row count should match Bronze 
# MAGIC SELECT 
# MAGIC     'bronze' AS layer,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM sales_pipeline_analytics.bronze.raw_deal_events
# MAGIC UNION ALL 
# MAGIC SELECT 
# MAGIC     'silver' AS layer,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM sales_pipeline_analytics.silver.deal_events_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 2. Quality flag distribution 
# MAGIC SELECT 
# MAGIC     _quality_flags,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM sales_pipeline_analytics.silver.deal_events_cleaned
# MAGIC GROUP BY _quality_flags
# MAGIC ORDER BY cnt DESC;
# MAGIC
# MAGIC
# MAGIC