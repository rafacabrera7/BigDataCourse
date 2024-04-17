from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, explode, split, lit, concat_ws
import time

# Tiempo de inicio
start_time = time.time()

# Iniciamos la sesión de Spark
spark = SparkSession.builder.appName("lab_APISpark").getOrCreate()

# Iniciamos el contexto de Spark
sc = spark.sparkContext

# Leemos el archivo movies.csv
df_movies = spark.read.options(delimiter=",", header=True).csv("input/ml-25m/movies.csv")

# Explotamos la columna genres para obtener un dataframe con una fila por cada género de película
movies_long = df_movies.select("movieId", explode(split(df_movies.genres, "\|")).alias("genre"))

# Leemos el archivo ratings.csv
df_rating = spark.read.options(delimiter=",", header=True).csv("input/ml-25m/ratings.csv")

# Calculamos el promedio de rating por película
df_rating_per_movie = df_rating.groupBy("movieId").agg(avg("rating").alias("avg_rating"))

# Unimos el dataframe de películas con el dataframe de rating por película
df_movie_genre_rating = movies_long.join(df_rating_per_movie, movies_long.movieId == df_rating_per_movie.movieId, "left")

# Calculamos el promedio de rating por género
df_agrupado_por_genero = df_movie_genre_rating.groupBy("genre").agg(avg("avg_rating").alias("avg_rating"), count(lit(1)))
# df_agrupado_por_genero.show()

# Guardamos el resultado en un archivo de texto
df_agrupado_por_genero.select(concat_ws(",", df_agrupado_por_genero.genre, df_agrupado_por_genero.avg_rating, df_agrupado_por_genero["count(1)"]). \
                              alias("GenreAvg")).write.mode("overwrite").text("3_out.txt")

# Tiempo de finalización
end_time = time.time()

# Tiempo total de ejecución
print('-----------------------------------------\n')
print("Tiempo de ejecución: ", end_time - start_time)
print('\n-----------------------------------------')

## Para chequear el número correcto de películas por género.
#df_agrupado_por_genero1 = movies_long.groupBy("genre").count()
#df_agrupado_por_genero1.printSchema()
#df_agrupado_por_genero1.show()