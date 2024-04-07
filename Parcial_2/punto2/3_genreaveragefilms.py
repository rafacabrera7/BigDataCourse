import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import avg, explode, split, countDistinct
import time

# Tiempo de inicio
start_time = time.time()

# Creación de una sesión de Spark
spark = SparkSession.builder.appName("lab_APISpark").getOrCreate()
# Creación de un contexto de Spark
sc = spark.sparkContext

# Cambiamos el esquema de las columnas de los datasets
movie_schema = StructType().add("movieId", "integer").add("title", "string").add("genres", "string")
rating_schema = StructType().add("userId", "integer").add("movieId", "integer").add("rating", "double").add("timestamp", "integer")

# Importamos los datasets
df_movies = spark.read.format("csv").option("header", "true").schema(movie_schema). \
            load("/home/laura/Desktop/univ/univ2024-1/BigData/lab/LabSpark2/ml-25m/movies.csv")
df_ratings = spark.read.format("csv").option("header", "true").schema(rating_schema). \
            load("/home/laura/Desktop/univ/univ2024-1/BigData/lab/LabSpark2/ml-25m/ratings.csv")

# Hacemos uso de explode para manejar la columna 'genres' de df_movies, 
# asi cada película se divide en varias filas, una por cada género
movies_exploded = df_movies.withColumn("genre", explode(split("genres", "\|")))

# Unimos los datasets por la columna 'movieId'
# para tener las calificaciones de las películas por género
joined_data = df_ratings.join(movies_exploded, "movieId")

# Ahora calculemos la calificación promedio de las películas y su conteo por género
results = joined_data.groupBy("genre").agg(avg("rating").alias("avg_rating"), \
                      countDistinct("movieId").alias("count_movies"))

results.show(10, truncate=False)
# Guardamos los resultados 

# Tiempo de finalización
end_time = time.time()

# Tiempo total de ejecución
print('-----------------------------------------\n')
print("Tiempo de ejecución: ", end_time - start_time)
print('\n-----------------------------------------')


