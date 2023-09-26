import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F


spark = (SparkSession.builder
         .appName("Read Delta Table")
         .master("local[2]")
         .config("spark.executor.memory", "3g")
         .config("spark.driver.memory", "1g")
         .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .enableHiveSupport()
         .getOrCreate())

spark.sparkContext.setLogLevel('ERROR')

deltaPathSensor4d = "hdfs://localhost:9000/user/train/deltaPathSensor4d"

delta_table = spark.read.format("delta").load(deltaPathSensor4d)

print("row count ", delta_table.count())

delta_table.show()
