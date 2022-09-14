from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir","/user/hive/warehouse") \
    .getOrCreate()
	# Create DataFrame representing the stream of input lines from connection to localhost:9999
spark.sparkContext.setLogLevel("ERROR")
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

def func(batch_df, batch_id):
    print('Writing to hive {}'.format(batch_id))
    batch_df.write.mode("append").saveAsTable("hivewordcount")

# Start running the query that prints the running counts to the console 
query = wordCounts.writeStream.foreachBatch(func).outputMode("update").start()
query.awaitTermination()


