
import sys, string
import os
import socket
import time
import operator
import boto3
import json

from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import split, concat_ws, collect_list
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, TimestampType, StructField
from pyspark.sql.functions import col, count, lit
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
    
    data = spark.readStream.format("socket").option("host", 'localhost')\
            .option("port", 5050).load()

    data = data.withColumn("valores", split(data.value,','))
    
    dataClean = data.withColumn('id', data['valores'].getItem(0)) \
       .withColumn('content', data['valores'].getItem(1)) \
        .drop(data['valores'])\
        .drop(data['value'])
    
    dataClean = dataClean.filter(col('content').isNotNull() & (col('content') != ""))

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
    """
    def concatEnCadaBatch(df, batch):
        df = df.groupby().agg(concat_ws("********************************************",
                                            collect_list(df.content)))

        df.show(truncate=False)  # This will display the DataFrame in the terminal
        #llamadoaLLM(celda)
        #guardarrespuesta de llamado en S3
        #df.coalesce(1).write.text(f"s3://outputparcialstreaming/output/batch_{batch}")
    
    query = dataClean.writeStream.queryName("queryProyectoFinal") \
                          .outputMode("update") \
                          .format("console") \
                          .foreachBatch(concatEnCadaBatch) \
                          .trigger(processingTime='10 seconds')\
                          .option("truncate", "false").start()

    #run the query 
    query.awaitTermination(timeout = 100)
    query.stop()
