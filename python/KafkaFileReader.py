#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 KafkaWordCount.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,explode
from pyspark.sql.functions import split,lit,avg,col
from pyspark.sql.types import StringType
import uuid

checkpointLocation ="/tmp/temporary-" + str(uuid.uuid4())

spark = SparkSession.builder.appName("KafkaWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option('subscribe', "Hello-Kafka").load().selectExpr("CAST(value AS STRING)")
      
lines.writeStream.outputMode("append").format("console").start()

cols= split(lines['value'],'\t')
rawDF = lines.withColumn("empid",cols.getItem(0).cast("int"))\
             .withColumn("first_name",cols.getItem(1))\
             .withColumn("last_name",cols.getItem(2))\
             .withColumn("gender",cols.getItem(3))\
             .withColumn("emailid",cols.getItem(4))\
             .withColumn("doj",cols.getItem(5))\
             .withColumn("age",cols.getItem(6).cast("int"))\
             .withColumn("salary",cols.getItem(7).cast("double"))\
             .withColumn("dept_id",cols.getItem(8).cast("int"))\
             .drop("value")

random_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()             
avgSalDF =rawDF.groupBy("dept_id").agg(avg(col("salary")))  
summaryDF = avgSalDF.withColumnRenamed("avg(salary)","Average Salary")\
        .withColumn("UUID",lit(random_udf()))
    
summaryDF.writeStream.outputMode("update").format("console").start()

query = lines \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
