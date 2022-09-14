from pyspark.sql import *
from pyspark.sql.types import *
userSchema = StructType().add("name", "string").add("age", "integer")
spark = SparkSession.builder.master("local[2]").appName("Spark Dir").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
csvDF = spark.readStream\
  .option("sep", ",")\
  .schema(userSchema)\
  .csv("hdfs://localhost:9000/user/ubuntu/inputdir")    
query = csvDF.writeStream.outputMode("append").format("console").start()
query.awaitTermination()	

