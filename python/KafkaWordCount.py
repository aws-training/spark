#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 KafkaWordCount.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import uuid
checkpointLocation ="/tmp/temporary-" + str(uuid.uuid4())

spark = SparkSession.builder.appName("KafkaWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option('subscribe', "Hello-Kafka").load().selectExpr("CAST(value AS STRING)")
      

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

 # Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
