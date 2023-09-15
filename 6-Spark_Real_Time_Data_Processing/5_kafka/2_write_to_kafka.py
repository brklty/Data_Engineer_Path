# Write to Kafka

import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("Write to Kafka")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")


lines = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test1")
    .load()
)

lines2 = lines.selectExpr("CAST(key as STRING)", "CAST(value as STRING)")


# write to console

streamingQuery = (lines2.writeStream
                  .format("kafka")
                  .outputMode("append")
                  .trigger(processingTime="2 seconds")
                  .option("checkpointLocation", "file:///tmp/streaming/checkpoint_write_kafka")
                  .option("truncate", False)
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("topic", "test2")
                  .start()
                  )

streamingQuery.awaitTermination()