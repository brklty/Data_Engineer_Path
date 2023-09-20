import findspark
findspark.init("/opt/manual/spark")
from pyspark.sql import SparkSession, functions as F

# SparkSession
spark = (SparkSession.builder
         .master("yarn")
         .appName("Spark Streaming Upsert Delta")
         .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.memory.fraction", "0.1")
         .config("spark.memory.storageFraction", "0.0")
         .getOrCreate())

spark.sparkContext.setLogLevel('ERROR')

from delta.tables import *

# Read Data

customer_schema = "customerId int, customerFName string, customerLName string,customerCity string, time timestamp"

deltaPath = "hdfs://localhost:9000/user/train/delta-stream-customers"

delta_table = DeltaTable.forPath(spark, deltaPath)


lines_from_file = (
    spark.readStream
    .format("csv")
    .option("header", False)
    .schema(customer_schema)
    .option("maxFilesPerTrigger", 1)
    .load("file:///home/train/data-generator/output"))


def upsertToDelta(update_df, batchId):
    update_df.persist()
    update_df.show(4)

    delta_table.alias("t") \
        .merge(update_df.alias("s"), "t.customerId = s.customerId") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    update_df.unpersist()

streamingQuery = (lines_from_file
                  .writeStream
                  .format("delta")
                  .outputMode("update")
                  .trigger(processingTime="5 seconds")
                  .foreachBatch(upsertToDelta)
                  .option("checkpointLocation", "/user/train/delta_customer_upsert_checkpoint")
                  .start()
                  )

streamingQuery.awaitTermination()



