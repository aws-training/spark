#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 KafkaWordCount.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Kafka JSON Parser").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option('subscribe', "Hello-Kafka").load().selectExpr("CAST(value AS STRING)")
      

from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([ 
    StructField("Zipcode",StringType(),True), 
    StructField("ZipCodeType",StringType(),True), 
    StructField("City",StringType(),True), 
    StructField("State", StringType(), True)
  ])
from pyspark.sql.functions import col,from_json
dfJSON = lines.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")
dfJSON.printSchema()
 # Start running the query that prints the running counts to the console
query = dfJSON \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
