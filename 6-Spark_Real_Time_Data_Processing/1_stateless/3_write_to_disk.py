### Write Results to Disk

import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("Write Results to Disk").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


#5.1,3.5,1.4,0.2,Iris-setosa,


iris_schema = "row_id int, SepalLengthCm float, SepalWidthCm float, PetalLengthCm float, PetalWidthCm float, Species string,time timestamp "

# read file stream

lines = (spark.readStream
         .format("csv")
         .schema(iris_schema)
         .option("header", False)
         .option("maxFilesPerTrigger", 1)
         .load("file:///home/train/data-generator/output"))

# operation and transformation

#lines2 = lines.filter(" Species == 'Iris-setosa' ")
lines2 = lines.withColumn("row_id_100",F.col("row_id") * 100)


# write file stream to a sink

streamingQuery = (
lines2.writeStream
    .format('parquet')
    .outputMode('append')
    .trigger(processingTime="2 seconds")
    .option("checkpointLocation", "checkpoint-iris")
    .option("numRows", 4)
    .option("truncate", False)
    .option("path", "file:///home/train/my_pyspark/spark_real_time_data_processing/stateless/output")
    .start()
)

streamingQuery.awaitTermination(100)

