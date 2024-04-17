import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import regexp_replace, col, when, concat_ws, explode, split, max
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

# Limpiar la columna Team, necesitamos quitar los []
# "['Nintendo', 'Game Freak']" -> "Nintendo, Game Freak"
df = df.withColumn("teams", regexp_replace(col("Team"), "\[|\]|\'", ""))

# Limpiar la columna Playing, necesitamos quitar los K y convertir a float
df = df.select("Title","genres","Playing", "teams", 
                   when(col("Playing").contains("K"), 
                        (regexp_replace(col("Playing"), "K$", "").cast("float") * 1000))
                   .otherwise(col("Playing").cast("float")).alias("playing_fixed"))


##############################################################
### Cálculos para el video juego más popular de cada género###
##############################################################

df1 = df.select("Title","playing_fixed", explode(split(df.genres, ", ")).alias("genre"))
# visualizamos con el sort para asegurarnos que la respuesta final coincida.
# df1.sort(df1.playing_fixed.desc()).show()

# Calculamos el videojuego más popular de cada género
df_genre_max = df1.groupBy("genre").agg(max("playing_fixed").alias("max_playing"))
df_genre_max = df_genre_max.withColumnRenamed("genre", "max_genre")
#df_genre_max_title.show()

# Unimos el dataframe original con el dataframe de los videojuegos más populares por género
df_genre_max_with_title = df1.join(
    df_genre_max,
    (df1.genre == df_genre_max.max_genre) &
    (df1.playing_fixed == df_genre_max["max_playing"]),
    "inner"
).select(df1["Title"], df1["genre"], df_genre_max["max_playing"].alias("popularidad"))

df_genre_max_with_title = df_genre_max_with_title.dropDuplicates(["genre"])
# df_genre_max_with_title.sort(df_genre_max_with_title.popularidad.desc()).show(30)

# Guardamos el resultado en un archivo de texto
df_genre_max_with_title.repartition(2).select(concat_ws(",", col("Title"), col("genre"), col("popularidad")).alias("GenderMax")) \
    .write.mode("overwrite").text("4_genreout.txt")



##############################################################
### Cálculos para el video juego más popular de cada team###
##############################################################

df2 = df.select("Title","playing_fixed", explode(split(df.teams, ", ")).alias("team"))
# visualizamos con el sort para asegurarnos que la respuesta final coincida.
# df2.sort(df2.playing_fixed.desc()).show()

# Calculamos el videojuego más popular de cada team
df_team_max = df2.groupBy("team").agg(max("playing_fixed").alias("max_playing"))
df_team_max = df_team_max.withColumnRenamed("team", "max_team")
#df_team_max_title.show()

# Unimos el dataframe original con el dataframe de los videojuegos más populares por team
df_team_max_with_title = df2.join(
    df_team_max,
    (df2.team == df_team_max.max_team) &
    (df2.playing_fixed == df_team_max["max_playing"]),
    "inner"
).select(df2["Title"], df2["team"], df_team_max["max_playing"].alias("popularidad"))

df_team_max_with_title = df_team_max_with_title.dropDuplicates(["team"])
# df_team_max_with_title.sort(df_team_max_with_title.popularidad.desc()).show(100)

# Guardamos el resultado en un archivo de texto
df_team_max_with_title.repartition(2).select(concat_ws(",", col("Title"), col("team"), col("popularidad")).alias("TeamMax")) \
    .write.mode("overwrite").text("4_teamout.txt")

# Tiempo de finalización
end_time = time.time()

# Tiempo total de ejecución
print('-----------------------------------------\n')
print("Tiempo de ejecución: ", end_time - start_time)
print('\n-----------------------------------------')