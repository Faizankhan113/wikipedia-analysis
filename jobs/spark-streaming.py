from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window ,from_unixtime, to_timestamp, round, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, LongType


# -----------------------
# Spark Session
# -----------------------

spark = (
    SparkSession.builder
    .appName("WikiKafkaToCSV")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set(
    "spark.sql.streaming.statefulOperator.checkCorrectness.enabled",
    "false"
)
spark.conf.set("spark.sql.shuffle.partitions", "1")


# -----------------------
# Read From Kafka
# -----------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wiki-changes")
    .option("startingOffsets", "latest")
    .load()
)

# ------------------------------------------------
# 3. Convert Kafka Value to String in jason type
# ------------------------------------------------

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json")

# -----------------------
# Schema
# -----------------------

schema = StructType([

    # Root fields
    StructField("type", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),

])



print('started')

# -----------------------
# Parse JSON
# -----------------------

parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")




parsed_df = parsed_df.withColumn('timestamp',to_timestamp(from_unixtime(col('timestamp')),))

parsed_df = parsed_df.withWatermark('timestamp','3 seconds')

grouped_df = parsed_df.groupBy(window('timestamp','10 seconds')).pivot('bot',[True,False]).count()

grouped_df = (
    grouped_df 
    .withColumn("start_time", col("window.start"))
    .withColumn("end_time", col('window.end'))
    .drop("window")
)

grouped_df = grouped_df.na.fill(0, ["true","false"])

grouped_df=grouped_df.withColumn('total', col('true') + col('false'))

grouped_df = grouped_df.withColumn(
    'bot%',
    when(col('total') == 0, 0)
    .otherwise(round((col('true') / col('total')) * 100, 1))
)

grouped_df = grouped_df.withColumn(
    'human%',
    when(col('total') == 0, 0)
    .otherwise(round((col('false') / col('total')) * 100, 1))
)
grouped_df=grouped_df.drop('true','false')


# -----------------------
# Write To CSV
# -----------------------

query = (
    grouped_df.writeStream
    .format("csv")
    .trigger(processingTime='10 seconds')
    .option("path", "/jobs/output_csv")
    .option("checkpointLocation", "/jobs/checkpoint_csv")
    .option("header", "true")
    .outputMode("append")
    .start()
)




print('ended')

query.awaitTermination()
