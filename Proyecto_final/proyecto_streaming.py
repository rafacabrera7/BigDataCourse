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
import openai

# Set your API key here or ensure it is set in your environment variables
api_key = 'sk-proj-JXQPlq4nvDfC2mc6pX7lT3BlbkFJIXNxkEcNz3JXW9PHOCIY'

# Initialize the OpenAI client
openai.api_key = api_key

# Define your model ID (ensure it matches your actual model ID)
model_id = "ft:gpt-3.5-turbo-0125:personal:proyectobigdata:9UPlgk35"

# Define the base prompt
base_prompt = "The input text below is a publication in Reddit from a user. Extract the videogame names mentioned in it. Return only a python style list with the videogames names in lower case as strings items. If you don't find any videogame name, return an empty list. Input text:"

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

    def llm_response(user_input):
        # Combine the base prompt with the user input
        prompt = f"{base_prompt} {user_input}"
        
        # Create the chat completion request
        response = openai.ChatCompletion.create(
            model=model_id,
            messages=[
                {"role": "system", "content": "Chatbot that identifies videogame names in text and only returns a python style list with the identified games in lower case letters."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.6,
            max_tokens=256,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        
        # Return the response content
        return response.choices[0].message.content

    # Example usage:
    #user_input = "should i get rocket league on pc or xbox go to rrocketleague and witness the likes of  https  gfycatcomshamelessinformalbats League of legends is awesome but I hate fall guys"
    #response = llm_response(user_input)
    #print(response)
    
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
        print(content_list)

        #llamadoaLLM(celda)
        games = llm_response(' '.join(content_list))
        print(games)

        #save_to_rds(games)
        #df.coalesce(1).write.text(f"s3://outputparcialstreaming/output/batch_{batch}")
    
    query = dataClean.writeStream.queryName("queryProyectoFinal") \
                          .outputMode("update") \
                          .format("console") \
                          .foreachBatch(concatEnCadaBatch) \
                          .trigger(processingTime='10 seconds')\
                          .option("truncate", "false").start()

    #run the query 
    query.awaitTermination()
    query.stop()
