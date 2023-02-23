import time
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import *
from pyspark.sql.types import *


load_dotenv()

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
MONGO_URL = os.getenv("MONGO_URL")
KAFKA_TOPIC_NAME = "country_data"
spark = SparkSession.builder.appName("Insert to Mongodb")\
            .master("local[*]")\
            .config("spark.mongodb.input.uri", MONGO_URL) \
            .config("spark.mongodb.output.uri", MONGO_URL) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
            .getOrCreate()


spark.conf.set("spark.sql.streaming.schemaInference","true")

dataStream  = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)\
        .option("subscribe", KAFKA_TOPIC_NAME)\
        .option("startingOffsets", "latest")\
        .load()

print("Streaming DataFrame : ", dataStream.isStreaming)

sample_schema = (
        StructType()
        .add("Id", IntegerType())
        .add("Company_url", StringType())
        .add("Company_ID", IntegerType())
        .add("Company_Name", StringType())
        .add("Headquarters", StringType())
        .add("Website", StringType())
        .add("Employees", StringType())
        .add("Stock_Symbol", StringType())
        .add("Stock_Symbol", StringType())
        .add("Ticker", StringType())
        .add("Revenue", StringType())
        .add("Phone", StringType())
        .add("Siccode", StringType())
        .add("Naicscode", StringType())
        .add("Description", StringType())
        .add("Industry", StringType())
        .add("Company_Logo_URL", StringType())
    )

base_df = dataStream.selectExpr("CAST(value as STRING)", "timestamp")
base_df.printSchema()
info_dataframe = base_df.select(
        from_json(col("value"), sample_schema).alias("sample"), "timestamp"
    )
info_df_fin = info_dataframe.select("sample.*", "timestamp")
info_df_fin.printSchema()


def write_mongo_row(df, epoch_id): 
    print("DATA IS UPLOADING............")
    df.write.format("mongo").mode("overwrite").option("database","new_data").option("collection", "company").save()
    print("DATA HAS BEEN UPLOADED YOU CAN CLOSE THE TERMINAL")
    pass


info_df_fin.writeStream.foreachBatch(write_mongo_row).start().awaitTermination()
