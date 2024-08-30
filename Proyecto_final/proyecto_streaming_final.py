import csv
import os
import socket
import operator
import boto3
import json
import psycopg2
from psycopg2 import sql
import ast 
import time
import openai

from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import split, concat_ws, collect_list
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, TimestampType, StructField
from pyspark.sql.functions import col, count, lit


"""

El objetivo de este proyecto es desarrollar un pipeline de streaming que procese publicaciones de Reddit, 
extraiga los nombres de videojuegos mencionados en ellas utilizando un modelo de lenguaje de OpenAI, 
y almacene estos nombres en una base de datos PostgreSQL alojada en AWS RDS.

"""

# Aca configuramos la clave de la API de OpenAI
api_key = 'API_KEY_HERE_OR_ENV_VAR'

# Inicializamos el cliente de OpenAI
openai.api_key = api_key

# Definimos el ID del modelo (hay que asegurarse de que coincida con tu ID de modelo real)
model_id = "MODEL_ID_HERE"

# Definimos el prompt base para la extracción de nombres de videojuegos
base_prompt = "The input text below is a publication in Reddit from a user. Extract the videogame names mentioned in it. Return only a python style list with the videogames names in lower case as strings items. If you don't find any videogame name, return an empty list. Input text:"

print()
print("ESTE SÍ ES PROYECTO FINAL")
print()

if __name__ == "__main__":
    # Inicializamos la sesión de Spark
    spark = SparkSession\
        .builder\
        .appName("ParcialStreaming")\
        .getOrCreate() \
    
    # Configuramos el nivel de log de Spark
    spark.sparkContext.setLogLevel("ERROR")
    
    windowDuration = '60 seconds'
    
    # Leemos los datos del socket
    data = spark.readStream.format("socket").option("host", 'localhost')\
            .option("port", 5050).load()

    # Dividimos los datos en columnas
    data = data.withColumn("valores", split(data.value,','))
    
    # Seleccionamos las columnas de interés y eliminamos las columnas innecesarias
    dataClean = data.withColumn('id', data['valores'].getItem(0)) \
       .withColumn('content', data['valores'].getItem(1)) \
        .drop(data['valores'])\
        .drop(data['value'])
    
    # Filtramos los datos para eliminar los contenidos nulos o vacíos
    dataClean = dataClean.filter(col('content').isNotNull() & (col('content') != ""))


    """
    En este punto, ya hemos definido la estructura básica de nuestro pipeline de streaming.
    Ahora, necesitamos definir una función que se encargue de enviar las publicaciones de Reddit 
    al modelo de lenguaje de OpenAI,
    """
    def llm_response(user_input):
        try:
            # Verifica si la entrada del usuario no está vacía
            if user_input != '':
                # Combina el prompt base con la entrada del usuario
                prompt = f"{base_prompt} {user_input}"
                
                # Crea una solicitud de completitud al modelo de lenguaje de OpenAI
                response = openai.chat.completions.create(
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
            
                # Devuelve el contenido de la respuesta
                return response.choices[0].message.content
        except:
            pass
    

    """
    Ahora, necesitamos definir una función que se encargue de guardar los nombres de videojuegos
    extraídos por el modelo de lenguaje en una base de datos PostgreSQL alojada en AWS RDS.
    """   
    # Parámetros de conexión a la base de datos
    db_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'ferneytupapa',
        'host': 'proyectobigdata.cbfryhd4vdmu.us-east-1.rds.amazonaws.com',
        'port': '5432'
    }
    
    def get_rds_connection():
        try:
            # Intenta establecer la conexión con la base de datos
            con = psycopg2.connect(**db_params)
            return con
        except Exception as e:
            print(f"Error connecting to the database: {e}")
    
    def save_to_rds(data):
        try:
            # Obtenemos una conexión a la base de datos
            con = get_rds_connection()
            cur = con.cursor()
            
            # Insertamos los nombres de videojuegos en la tabla 'juegos' de la base de datos
            for game in data:
                cur.execute("INSERT INTO juegos (id_juego_mencion, juego) VALUES (default, %s)", (game,))
            
            # Confirmamos los cambios en la base de datos y cerramos la conexión
            con.commit()
            cur.close()
            con.close()
            print("Data saved to the database successfully.")
        except Exception as e:
            print(f"Error saving data to the database: {e}")


    """
    Ahora, necesitamos definir una función que se encargue de procesar cada lote de datos en el DataFrame
    y ejecutar las funciones de extracción de nombres de videojuegos y guardado en la base de datos.
    """
    def concat_in_each_batch(df, batch_id):
        # Concatenamos el contenido de todas las filas en un solo string
        df = df.groupby().agg(concat_ws("",
                                        collect_list(df.content)).alias("concatenate content"))

        df.show(truncate=False)  

        # Convertimos el contenido concatenado en una lista de strings
        content_list = df.select('concatenate content').rdd.flatMap(lambda x: x).collect()
        print("LLM input:")
        print(' '.join(content_list))

        # Llamamos al modelo de lenguaje de OpenAI para extraer los nombres de videojuegos
        games = llm_response(' '.join(content_list))

        if games != None:
            try:
                games = ast.literal_eval(games)

                print("LLM output")
                print(type(games))
                print(games)
                
                # Guardamos los nombres de videojuegos en la base de datos            
                save_to_rds(games)
            except:
                pass

    # Finalmente, definimos la consulta de streaming que ejecutará el pipeline completo.    
    query = dataClean.writeStream.queryName("queryProyectoFinal") \
                          .outputMode("update") \
                          .format("console") \
                          .foreachBatch(concat_in_each_batch) \
                          .trigger(processingTime='10 seconds')\
                          .option("truncate", "false").start()

    # Ejecutamos la consulta
    query.awaitTermination()
    query.stop()

"""    
Resultados de salidas:
- En la terminal de Spark, se mostrará el contenido de cada lote de datos procesado,
  así como las listas de nombres de videojuegos extraídos por el modelo de lenguaje.

- En la base de datos PostgreSQL alojada en AWS RDS, se almacenarán los nombres de 
  videojuegos extraídos. Estos nombres se guardarán en la tabla 'juegos' con un ID
  autoincremental y el nombre del videojuego en minúsculas. 

Finalmente, se pueden visualizar los resultados de la base de datos en pgAdmin, con
gráficos de barras, que muestren los videojuegos más mencionados en las publicaciones.
"""
