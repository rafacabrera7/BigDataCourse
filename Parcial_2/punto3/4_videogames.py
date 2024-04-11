import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import regexp_replace, col, when, concat_ws

# Crear una sesión de Spark
spark = SparkSession.builder.appName("APIVideoGames").getOrCreate()

# Crear un contexto de Spark
sc = spark.sparkContext

# Leer el archivo CSV
df = spark.read.format("csv").option("header", "true"). \
                       load("/home/laura/Desktop/univ/univ2024-1/BigData/proyecto/proyecto2/input/games.csv")

# Limpiar la columna Genres, necesitamos quitar los []
# "['Adventure', 'RPG']" -> "Adventure, RPG"
df = df.withColumn("Genres", regexp_replace(col("Genres"), "\[|\]", ""))

# Quitamos la k que acompaña el valor en la columna Playing
# lo convertimos a entero y si tiene k se multiplica por 1000
df = df.withColumn("CurrentPlaying", when(col("Playing").contains("K"), (regexp_replace(col("Playing"), "K", ""). \
                                   cast("float") * 1000)))

df.select(concat_ws(", ", col("Genres"), col("Playing"), col("Title")).alias("Info")).show(truncate=False)
