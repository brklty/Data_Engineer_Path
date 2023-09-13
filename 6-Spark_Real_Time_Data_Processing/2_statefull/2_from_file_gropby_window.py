import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("From CSV GroupBy Window").getOrCreate()

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


lines2 = lines.groupBy(
    F.window(F.col("time"), "2 minutes", "1 minutes"),
    F.col("Species")
).count()

streamingQuery = (
lines2.writeStream
    .format('console')
    .outputMode('complete')
    .trigger(processingTime="1 seconds")
    .option("checkpointLocation", "file:///home/train/checkpoint-groupby_windows")
    .option("numRows", 4)
    .option("truncate", False)
    .start()
)

streamingQuery.awaitTermination(100)

