# Read Batch

import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("Read Batch").getOrCreate()


spark.read.parquet("file:///home/train/my_pyspark/spark_real_time_data_processing/stateless/output").show()

