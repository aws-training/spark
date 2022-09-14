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
			.option("subscribe", "Hello-Kafka")\
			.load()


		# convert from bytes to string
rawDF = inputDF.selectExpr("CAST(value AS STRING)")
		# split each row on comma, load it to the case class
split_col = split(rawDF['value'], '\t')
df = rawDF.withColumn('empid', split_col.getItem(0).cast("int"))\
		    .withColumn('first_name', split_col.getItem(1))\
		    .withColumn('last_name', split_col.getItem(2))\
		    .withColumn('gender', split_col.getItem(3))\
                    .withColumn('email', split_col.getItem(4))\
                    .withColumn('doj', split_col.getItem(5))\
                    .withColumn('age', split_col.getItem(6).cast("int"))\
                    .withColumn('salary', split_col.getItem(7).cast("double"))\
                    .withColumn('dept_id', split_col.getItem(8).cast("int"))\
		    .drop("value")

		# groupby and aggregate
avgDf = df.groupBy("dept_id")\
			.agg(avg(col("salary")))


random_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()

avgSalDF = avgDf.withColumn('uuid', random_udf())\
			.withColumnRenamed("avg(salary)", "Average Salary")

query = avgSalDF.writeStream.format("console")\
			.outputMode("update")\
			.start()
query.awaitTermination()

