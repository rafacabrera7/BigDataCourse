import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import regexp_replace, col, when, concat_ws, explode, split, max, avg, length
import time

# Tiempo de inicio
start_time = time.time()

# Crear una sesión de Spark
spark = SparkSession.builder.appName("APIVideoGames").getOrCreate()

# Crear un contexto de Spark
sc = spark.sparkContext

df = spark.read.option("escape","\"").option("header", "true").option("escapeQuotes", "true").option("multiline", "true").csv("input/games.csv").repartition(2).cache()
#df.show()

# Limpiar la columna Genres, necesitamos quitar los []
# "['Adventure', 'RPG']" -> "Adventure, RPG"
df = df.withColumn("genres", regexp_replace(col("Genres"), "\[|\]|\'", ""))

# Limpiar la columna Reviews, necesitamos quitar los []
df = df.withColumn("clean_reviews", regexp_replace(col("Reviews"), "\[|\]|\'", ""))

# Limpiar la columna Playing, necesitamos quitar los K y convertir a float
df = df.select("Title","genres","Playing", "Rating", "clean_reviews",
                   when(col("Playing").contains("K"), 
                        (regexp_replace(col("Playing"), "K$", "").cast("float") * 1000))
                   .otherwise(col("Playing").cast("float")).alias("playing_fixed"))

# Explotamos la columna genres para obtener un dataframe con una fila por cada género de juego
df1 = df.select(col("Rating"), explode(split(df.genres, ", ")).alias("genre"),col("clean_reviews") )

# Calcular el promedio del rating y la longitud de las reviews de cada género de juego
avg_rating_df = df1.groupBy("genre").agg(avg("Rating").alias("avg_rating"),avg(length(col("clean_reviews"))).alias("avg_reviews"))

# Unir el nombre del género con el promedio del rating y de la longitud de las reviews
# y guardar el resultado en dos partes
avg_rating_df.repartition(2).select(concat_ws(",",col("genre"),col("avg_rating"),col("avg_reviews")).alias("AllAvg")) \
    .write.mode("overwrite").text("5_out.txt")
avg_rating_df.show()

# Tiempo de finalización
end_time = time.time()

# Tiempo total de ejecución
print('-----------------------------------------\n')
print("Tiempo de ejecución: ", end_time - start_time)
print('\n-----------------------------------------')