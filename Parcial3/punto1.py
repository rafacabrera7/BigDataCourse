from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("parcial_streaming").getOrCreate()

lines = (spark.readStream
         .format("socket")
         .option("host", "0.tcp.ngrok.io")
         .option("port", 16039)
         .option("escapeQuotes", "true")
         .option("multiline", "true")
         .load())

#words = lines.select(split(col("value"),"\\s").alias("word"))
#counts = words.groupBy("word").count()

checkpointDir = "..."
streamingQuery = (lines.writeStream
                  .format("console")
                  .outputMode("update")
                  .trigger(processingTime="5 seconds")
                  .option("checkpointLocation", checkpointDir)
                  .start())

streamingQuery.awaitTermination()
