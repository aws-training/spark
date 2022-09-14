from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Print Error Lines").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 7777).load()

errorLines = lines.filter(lines.value=="error")

query = errorLines.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
