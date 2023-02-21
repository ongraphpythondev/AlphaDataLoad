import time
import os
from pyspark.sql import SparkSession
from convert_to_csv import convert
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")
spark = SparkSession.builder.appName("Upload to Mongodb")\
            .master("local[*]")\
            .config("spark.mongodb.input.uri", MONGO_URL) \
            .config("spark.mongodb.output.uri", MONGO_URL) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()


spark.conf.set("spark.sql.streaming.schemaInference","true")

convert()
dataStream  = spark.readStream\
    .format("csv")\
    .option("inferSchema","true")\
    .option("header", "true")\
    .option("maxFilesPerTrigger", 1)\
    .load("./data")

dataStream.printSchema()
print("Streaming DataFrame : ", dataStream.isStreaming)


def write_mongo_row(df, epoch_id): 
    print("uploading data")
    df.write.format("mongo").mode("overwrite").option("database","load_data").option("collection", "country").save()
    print("Uploaded data")
    pass


query = dataStream.writeStream.foreachBatch(write_mongo_row).start()

print(query.status)
time.sleep(40)
print(query.status)
query.stop()
