from pyspark.sql import SparkSession
def hdfs_task():
	sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
	df = sparkSession.read.csv("hdfs://namenode:9000/nn_all.csv")
	df.write.mode("overwrite").csv("hdfs://namenode:9000/vacancy.csv")

