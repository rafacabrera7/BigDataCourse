from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, udf
from pyspark.sql.types import StringType
import re

# Define a function to handle multiline input and remove extra whitespaces
def process_multiline(data):
    # Combine the rows into a single string
    combined_text = ' '.join(data)
    # Remove leading and trailing whitespaces
    combined_text = combined_text.strip()
    # Replace multiple whitespaces with a single whitespace
    combined_text = re.sub(r'\s+', ' ', combined_text)
    return combined_text

if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("ParcialStreaming")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    windowDuration = '50 seconds'
    slideDuration = '25 seconds'
    
    # Register the process_multiline function as a UDF (User Defined Function)
    spark.udf.register("process_multiline_udf", process_multiline, StringType())
    
    # Read data from socket and apply the UDF for multiline processing
    data = spark.readStream.format("socket")\
        .option("host", '0.tcp.ngrok.io')\
        .option("port", 16039)\
        .load()\
        .withColumn("processed_value", expr("process_multiline_udf(value)"))\
        .drop("value")\
        .withColumnRenamed("processed_value", "value")

    query = data.writeStream.outputMode("update")\
                       .option("numRows", "100000")\
                       .option("truncate", "false")\
                       .format("console")\
                       .start()

    query.awaitTermination()
