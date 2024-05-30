import csv
import sys, string
import os
import socket
import time
import operator
import boto3
import json
import psycopg2
from psycopg2 import sql

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
            .option("port", 5432).load()

    data = data.withColumn("valores", split(data.value,','))
    
    dataClean = data.withColumn('id', data['valores'].getItem(0)) \
       .withColumn('content', data['valores'].getItem(1)) \
        .drop(data['valores'])\
        .drop(data['value'])
    
    dataClean = dataClean.filter(col('content').isNotNull() & (col('content') != ""))

    def llm_response(content_list):
        games = ['lol', 'csgo', 'minecraft']
        return games 
    
    db_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'ferneytupapa',
        'host': 'proyectobigdata.cbfryhd4vdmu.us-east-1.rds.amazonaws.com',
        'port': '5432'
    }
    
    def get_rds_connection():
        try:
            con = psycopg2.connect(**db_params)
            return con
        except Exception as e:
            print(f"Error connecting to the database: {e}")
    
    def save_to_rds(data):
        try:
            con = get_rds_connection()
            cur = con.cursor()
            
            for game in data:
                cur.execute("INSERT INTO juegos (id_juego_mencion, juego) VALUES (default, %s)", (game,))
            
            con.commit()
            cur.close()
            con.close()
            print("Data saved to the database successfully.")
        except Exception as e:
            print(f"Error saving data to the database: {e}")

    def concatEnCadaBatch(df, batch):
        df = df.groupby().agg(concat_ws("",
                                        collect_list(df.content)).alias("concatenate content"))

        df.show(truncate=False)  # This will display the DataFrame in the terminal

        content_list = df.select('concatenate content').rdd.flatMap(lambda x: x).collect()
        #llamadoaLLM(celda)
        games = llm_response(content_list)

        save_to_rds(games)
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
