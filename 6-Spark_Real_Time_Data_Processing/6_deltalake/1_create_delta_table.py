import findspark
findspark.init("/opt/manual/spark")
from pyspark.sql import SparkSession, functions as F

# SparkSession
spark = (SparkSession.builder
         .master("yarn")
         .appName("Create Delta Table")
         .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

spark.sparkContext.setLogLevel('ERROR')

from delta.tables import *

# Read Data

customer_schema = "customerId int, customerFName string, customerLName string,customerCity string, time timestamp"

customer_batch_df = (
    spark.read
    .format("csv")
    .schema(customer_schema)
    .load("file:///home/train/data-generator/output"))

#customer_batch_df.show()

#print(customer_batch_df.count())

# Delta Table Path
deltaPath = "hdfs://localhost:9000/user/train/delta-stream-customers"
customer_batch_df.write.format("delta")\
    .mode("overwrite")\
    .save(deltaPath)

delta_table = DeltaTable.forPath(spark, deltaPath)

delta_table.toDF().show()
print(delta_table.toDF().count())