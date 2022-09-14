#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 StreamHandler.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,split,lit,avg,col
from pyspark.sql.types import StringType
import uuid
spark = SparkSession.builder\
			.appName("Stream Handler")\
			.enableHiveSupport()\
			.config("spark.sql.warehouse.dir","/user/hive/warehouse")\
			.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

inputDF = spark	.readStream\
			.format("kafka")\
			.option("kafka.bootstrap.servers", "localhost:9092")\
			.option("subscribe", "weather")\
			.load()


		# convert from bytes to string
rawDF = inputDF.selectExpr("CAST(value AS STRING)")

		# split each row on comma, load it to the case class
split_col = split(rawDF['value'], ',')
df = rawDF.withColumn('device', split_col.getItem(0))\
		    .withColumn('temp', split_col.getItem(1).cast("double"))\
		    .withColumn('humid', split_col.getItem(2).cast("double"))\
		    .withColumn('pres', split_col.getItem(3).cast("double"))\
			.drop("value")

		# groupby and aggregate
summaryDf = df.groupBy("device")\
			.agg(avg(col("temp")), avg(col("humid")), avg(col("pres")))

random_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()


summaryWithIDs = summaryDf.withColumn('uuid', random_udf())\
			.withColumnRenamed("avg(temp)", "temp")\
			.withColumnRenamed("avg(humid)", "humid")\
			.withColumnRenamed("avg(pres)", "pres")\

summaryWithIDs.writeStream.outputMode("complete").format("console").start()
def func(batchDF, batchID):
	print("Writing to hive", batchID)
	batchDF.write.mode("append").saveAsTable("weather")

		# write dataframe to Hive
query = summaryWithIDs.writeStream.foreachBatch(func)\
			.outputMode("update")\
			.start()


query.awaitTermination()

