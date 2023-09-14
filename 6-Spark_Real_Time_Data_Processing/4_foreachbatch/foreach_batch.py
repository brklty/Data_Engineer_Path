# Foreachbatch

import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("Foreach Batch")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

iris_schema = "row_id int, SepalLengthCm float, SepalWidthCm float, PetalLengthCm float, PetalWidthCm float, " \
              "Species string,time timestamp "

lines = (
    spark.readStream
    .format("csv")
    .schema(iris_schema)
    .option("header", False)
    .option("maxFilesPerTrigger", 1)
    .load("file:///home/train/data-generator/output")
)

jdbcUrl = "jdbc:postgresql://localhost/traindb?user=train&password=Ankara06"

def write_to_multiple_sink(df, batchId):

    df.show()

    # write to postgresql
    df.write.jdbc(url=jdbcUrl,
                  table="iris_stream",
                  mode="append",
                  properties={"driver":"org.postgresql.Driver"})

    # write to local file
    df.write \
        .format("parquet") \
        .mode("append") \
        .save("file:///home/train/my_pyspark/spark_real_time_data_processing/4_foreachbatch/iris_stream_parquet")


# start streaming
streamingQuery = (lines
                  .writeStream
                  .foreachBatch(write_to_multiple_sink)
                  .option("checkpointLocation", "file:///tmp/streaming/from_csv_to_multiple_sink_foreach_batch")
                  .start())


streamingQuery.awaitTermination()


