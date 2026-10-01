# Databricks notebook source
'''
Setup Unity Catalog 
'''

spark.sql("CREATE CATALOG IF NOT EXISTS sales_pipeline_analytics")
spark.sql("USE CATALOG sales_pipeline_analytics")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

spark.sql("CREATE VOLUME IF NOT EXISTS sales_pipeline_analytics.bronze.deal_events")
spark.sql("CREATE VOLUME IF NOT EXISTS sales_pipeline_analytics.bronze.checkpoints")

BRONZE_TABLE = "sales_pipeline_analytics.bronze.raw_deal_events"
CHECKPOINT_PATH = "/Volumes/sales_pipeline_analytics/bronze/checkpoints/deal_events"

print("UC Volumes created.")
print(f"    -> Bronze table: {BRONZE_TABLE}")
print(f"    -> Checkpoint: {CHECKPOINT_PATH}")

# COMMAND ----------

'''
Confluent Cloud connection settings
'''
# Confluent Cloud connection settings
KAFKA_BOOTSTRAP = "pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092"
KAFKA_API_KEY = "K6HRZMRBR266OF7J"
KAFKA_API_SECRET = "cfltgv5vPU/nVzybhcaxPYKWtTOLykvcflvDBrW6y1wVI2B2PxwcxJCzuH54GvqA"
KAFKA_TOPIC = "deal_events"



# COMMAND ----------

'''
Read stream from Kafka
'''
# Read stream from Kafka
raw_stream = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config",
                        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
                        f'username="{KAFKA_API_KEY}" '
                        f'password="{KAFKA_API_SECRET}";')
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .load()
            )

print(f"Stream created - reading from topic: {KAFKA_TOPIC}")



# COMMAND ----------

'''
Transform to Bronze schema
'''
from pyspark.sql.functions import col, current_timestamp, cast

# Transform to Bronze schema
bronze_df = (raw_stream
             .selectExpr(
                 "CAST(key AS STRING) AS deal_id",
                 "CAST(value AS STRING) AS raw_payload",
                 "topic",
                 "partition AS _kafka_partition",
                 "offset AS _kafka_offset",
                 "timestamp AS _kafka_timestamp" 
             )
             .withColumn("_ingested_at", current_timestamp())
)






# COMMAND ----------

'''
Write to Delta Lake table (Bronze)
'''

# 
bronze_query = (bronze_df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", CHECKPOINT_PATH)
                .option("mergeSchema", "true")
                .trigger(availableNow=True)
                .toTable(BRONZE_TABLE)
)

bronze_query.awaitTermination()

print(f"Streaming complete to: {BRONZE_TABLE}")
print(f"    -> Checkpoint: {CHECKPOINT_PATH}")


# COMMAND ----------

'''
Verify Bronze table 
'''
bronze_check = spark.read.table(BRONZE_TABLE)
bronze_check.show(5, truncate=False)
print(f"Total rows in Bronze: {bronze_check.count()}")