import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, to_timestamp, window, coalesce, lit
from pyspark.sql.functions import sum as spark_sum
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HTTPLogCounter") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic3") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse logs
parsed_df = kafka_df.select(
    regexp_extract(col("value").cast("string"), 
        r"\[([A-Za-z]{3},\s\d{1,2}\s[A-Za-z]{3}\s\d{4}\s\d{2}:\d{2}:\d{2}\sGMT)\]", 1
    ).alias("timestamp_string"),
    regexp_extract(col("value").cast("string"), 
        r"\]\s+(GET|POST)\s+", 1
    ).alias("operation"),
    regexp_extract(col("value").cast("string"), 
        r"\s(\d{3})\s+\d+$", 1
    ).cast("integer").alias("status_code")
)

# Timestamp conversion
timestamp_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp_string"), "EEE, dd MMM yyyy HH:mm:ss z")
).filter(
    col("timestamp").isNotNull() &
    col("operation").isin(["GET", "POST"]) &
    col("status_code").isNotNull()
)

# Categorization
categorized_df = timestamp_df.withColumn(
    "category",
    when(
        (col("operation") == "GET") & (col("status_code").between(200, 299)),
        "GET_success"
    ).when(
        (col("operation") == "GET") & (~col("status_code").between(200, 299)),
        "GET_failure"
    ).when(
        (col("operation") == "POST") & (col("status_code").between(200, 299)),
        "POST_success"
    ).otherwise("POST_failure")
)

# Changed to 5-minute window with 10-minute watermark
windowed_counts = categorized_df \
    .withWatermark("timestamp", "10 minutes").groupBy(
        window(col("timestamp"), "5 minutes"),  # 5-minute window
        "category"
    ) \
    .count()

# Aggregation (unchanged)
final_counts = windowed_counts.groupBy("window").agg(
    coalesce(spark_sum(when(col("category") == "GET_success", col("count"))), lit(0)).alias("GET_success"),
    coalesce(spark_sum(when(col("category") == "GET_failure", col("count"))), lit(0)).alias("GET_failure"),
    coalesce(spark_sum(when(col("category") == "POST_success", col("count"))), lit(0)).alias("POST_success"),
    coalesce(spark_sum(when(col("category") == "POST_failure", col("count"))), lit(0)).alias("POST_failure")
)

def write_to_csv(batch_df, batch_id):
    batch_df = batch_df.withColumn("batch_id", lit(batch_id))

    print(f"========= Batch ID: {batch_id} =========")
    batch_df.show(truncate=False)

    pandas_df = batch_df.toPandas()
    csv_file_path = "output_log_summary.csv"

    if os.path.exists(csv_file_path):
        pandas_df.to_csv(csv_file_path, mode='a', header=False, index=False)
    else:
        pandas_df.to_csv(csv_file_path, mode='w', header=True, index=False)

# Changed trigger to 5 minutes
# query = final_counts.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .trigger(processingTime="5 minutes").option("truncate", "false") \
#     .start()

query = final_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_csv) \
    .trigger(processingTime="5 minutes") \
    .start()

query.awaitTermination()
