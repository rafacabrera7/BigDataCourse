
import sys, string
import os
import socket
import time
import operator
import boto3
import json
# from pyspark.sql import SparkSession
# from pyspark.streaming import StreamingContext

from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import explode ,split ,window
from pyspark.sql.types import IntegerType, DateType, StringType, StructType
from pyspark.sql.functions import sum,avg,max, col, count

print()
print("ESTE SÍ ES")
print()
if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("ParcialStreaming")\
        .getOrCreate() \
    
    spark.sparkContext.setLogLevel("ERROR")
    # TODO: Define a windowDuration and slideDuration
    # Se definen dos variables windowDuration y slideDuration, 
    # que se utilizan más adelante en la consulta de Spark Streaming. 
    # Estas variables determinan la duración de la ventana y el deslizamiento
    # para el procesamiento de datos en tiempo real
    
    windowDuration = '50 seconds'
    slideDuration = '25 seconds'
        
    # Set up the `logsDF` readStream to take in data from a socket stream, AND include the timestamp
    # Transformación de  los datos leídos. Se divide cada línea de registro en palabras utilizando split()
    # y se crea una nueva columna "logs" con el resultado. Luego, se selecciona la columna de marca de tiempo 
    # (logsDF.timestamp) y se aplica la función explode() para convertir las palabras divididas en filas
    # separadas con la misma marca de tiempo.
    
    data = spark.readStream.format("socket").option("host", '6.tcp.ngrok.io')\
            .option("multiLine",True)\
            .option("port", 12160).load()

    data = data.withColumn("valores", split(data.value,','))
    """,Date Created,Number of Likes,Source of Tweet,Tweet,Sentiment"""

    dataClean = data.withColumn('id', data['valores'].getItem(0)) \
       .withColumn('date', data['valores'].getItem(1)) \
       .withColumn('likes', data['valores'].getItem(2)) \
        .withColumn('source', data['valores'].getItem(3)) \
        .withColumn('sentiment', data['valores'].getItem(5)) \
        .drop(data['valores'])\
        .drop(data['value'])\
        #.withColumn('Tweet', data['valores'].getItem(4)) \
    dataClean = dataClean.filter(col('id').rlike('^[0-9]+$'))

    ## Falta hacerle cast a la column date y luego agrupar por eso.

    result = dataClean.groupBy().agg(count(col('id')).alias('count'), sum('likes'), avg('likes'))
        
    # display all unbounded table completely while streaming the hostCountDF on console using output mode "complete"

    query = dataClean.writeStream.outputMode("update")\
                       .option("truncate", "false")\
                       .format("console")\
                       .start()

    ## Practice and observe by uncommenting the below code lines using different outputModel "append" , "update"
    
    ### display unbounded table withonly the changes are appended
    # query = hostCountsDF.writeStream.outputMode("append")\
    #                    .option("numRows", "100000")\
    #                    .option("truncate", "false")\
    #                    .format("console")\
    #                    .start()


    ### display unbounded with only the update 
    # query = hostCountsDF.writeStream.outputMode("update")\
    #                    .option("numRows", "100000")\
    #                    .option("truncate", "false")\
    #                    .format("console")\
    #                    .start()

    
    #run the query 
    query.awaitTermination()
