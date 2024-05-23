
import sys, string
import os
import socket
import time
import operator
import boto3
import json

from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import explode ,split ,window, concat_ws, collect_list
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, TimestampType
from pyspark.sql.functions import sum,avg,max, col, count
import time


print()
print("ESTE SÍ ES PROYECTO FINAL")
print()
if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("ParcialStreaming")\
        .getOrCreate() \
    
    spark.sparkContext.setLogLevel("ERROR")
    
    windowDuration = '60 seconds'
    
    data = spark.readStream.format("socket").option("host", '6.tcp.ngrok.io')\
            .option("multiLine",True)\
            .option("port", 13782).load()

    #data.value


    data = data.groupby().agg(concat_ws("AAAAA", collect_list(data.value  )))
    """
    
    dataClean = data.withColumn('id', data['valores'].getItem(0)) \
       .withColumn('date', data['valores'].getItem(1)) \
       .withColumn('likes', data['valores'].getItem(2)) \
        .drop(data['valores'])\
        .drop(data['value'])\

    dataClean = dataClean.filter(col('id').rlike('^[0-9]+$'))
    dataClean = dataClean.withColumn("id", dataClean["id"].cast(IntegerType())) \
                     .withColumn("likes", dataClean["likes"].cast(IntegerType())) \
                     .withColumn("date", dataClean["date"].cast(TimestampType()))

    result = dataClean.groupBy(window(dataClean.date, windowDuration, 0)) \
                  .agg(count("id").alias("count"), sum("likes").alias("total_likes"), avg("likes").alias("avg_likes"))

    def guardarBatch(df, batch):
        df = df.withColumn("window", df["window"].cast(StringType())) \
                     .withColumn("count", df["count"].cast(StringType())) \
                     .withColumn("total_likes", df["total_likes"].cast(StringType())) \
                     .withColumn("avg_likes", df["avg_likes"].cast(StringType()))
        
        df = df.select(concat_ws(",", *df.columns).alias("output_value"))
        df.coalesce(1).write.text(f"s3://outputparcialstreaming/output/batch_{batch}")
    """
    query = data.writeStream.queryName("queryProyectoFinal") \
                          .outputMode("update") \
                          .format("console") \
                          .option("truncate", "false") \
                          .start()
                          #.foreachBatch(guardarBatch) \
    
    #run the query 
    query.awaitTermination(timeout = 100)
    query.stop()
