# Databricks notebook source
# Silver: bronze.raw_deal_events -> silver.deal_events_cleaned
# Pattern: batch read of ALL bronze + overwrite -> idempotent 
BRONZE_TABLE = "sales_pipeline_analytics.bronze.raw_deal_events"
SILVER_TABLE = "sales_pipeline_analytics.silver.deal_events_cleaned"

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

spark.sql("CREATE SCHEMA IF NOT EXISTS sales_pipeline_analytics.silver")

# COMMAND ----------

'''
Build Silver Dataframe 
'''
from pyspark.sql.functions import (col, from_json, to_timestamp, current_timestamp,
                                   when, array, array_compact, lit, row_number, coalesce, concat_ws)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DoubleType
from pyspark.sql.window import Window

# 1.Read bronze table 
bronze_df = spark.read.table("sales_pipeline_analytics.bronze.raw_deal_events")

# 2. Parse JSON 
parsed_bronze_df = bronze_df.withColumn("parsed", from_json(col("raw_payload"), event_schema))

# 3. Deduplicate event_id
dedup_key = coalesce(
    col("parsed.event_id"), 
    concat_ws("-", lit("no_event_id"), col("_kafka_partition"), col("_kafka_offset"))
)

# 4. 
w = Window.partitionBy(dedup_key).orderBy(col("_kafka_timestamp"), col("_kafka_partition"), col("_kafka_offset"))

deduped_df= (parsed_bronze_df
                .withColumn("_rn", row_number().over(w))
                .filter(col("_rn") == 1)
                .drop(col("_rn"))
)

# 5. Extracting fields
silver_df = (deduped_df
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
                 current_timestamp().alias("_ingested_at"),
                 col("parsed").isNull().alias("_is_malformed")
            ))

'''
Build a quality flag column
'''
# 6. Quality flag
quality_flags = array(
    when(col("_is_malformed"), lit("malformed_json")),      
    when(~col("_is_malformed") & col("event_id").isNull(), lit("missing_event_id")),
    when(col("deal_value_rm").isNull(), lit("missing_deal_value")),
    when(col("new_stage").isNull(), lit("missing_stage")),
    when(col("event_timestamp").isNull(), lit("missing_timestamp")),
    when(
        (col("event_type") == "stage_changed") & (col("old_stage") == col("new_stage")), 
        lit("stage_unchanged")
    )
)

# Add quality flags to silver dataframe
silver_df = (silver_df
             .withColumn("_quality_flags", array_compact(quality_flags))
             .drop("_is_malformed")
)

'''
Write to silver table
'''
# 7. Write to silver table
(silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)

'''
Assertions
'''
# 8. Assertions: a failed check PAISES, so the task goes red and Airflow stop before dbt
silver = spark.read.table(SILVER_TABLE)
bronze_count = spark.read.table(BRONZE_TABLE).count()
silver_count = silver.count()
dup_event_ids = (silver
                 .filter(col("event_id").isNotNull())
                 .groupBy("event_id").count()
                 .filter(col("count") > 1)
                 .count()
)

errors = []
if silver_count == 0:
    errors.append("Silver is empty")
if silver_count > bronze_count:
    errors.append(f"Silver: ({silver_count}) > Bronze: ({bronze_count}): dedup or join bug")
if dup_event_ids > 0:
    errors.append(f"{dup_event_ids} duplicate event_id in Silver")

if errors: 
    raise ValueError("Silver quality check failed: " + "; ".join(errors))

print(
    f"Silver completed: bronze={bronze_count}, silver={silver_count}, "
    f"deduped={bronze_count - silver_count}"
)