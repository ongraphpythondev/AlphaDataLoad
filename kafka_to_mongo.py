import time
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient


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
        .add("Id", StringType())
        .add("Company_url", StringType())
        .add("Company_ID", StringType())
        .add("Company_Name", StringType())
        .add("Headquarters", StringType())
        .add("Website", StringType())
        .add("Employees", StringType())
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

base_df = dataStream.selectExpr("CAST(value as STRING)")
base_df.printSchema()
info_dataframe = base_df.select(
        from_json(col("value"), sample_schema).alias("sample")
    ).select("sample.*")

info_dataframe = info_dataframe.withColumn("Id",info_dataframe.Id.cast(IntegerType()))
info_dataframe = info_dataframe.withColumn("Company_ID",info_dataframe.Company_ID.cast(IntegerType()))

info_dataframe.printSchema()

count=0
def write_mongo_row(df): 
    cluster = MongoClient(MONGO_URL)
    db = cluster["new_data"]
    data = {
        "Id":df.Id,
        "Company_url": df.Company_url,
        "Company_ID": df.Company_ID,
        "Company_Name": df.Company_Name,
        "Headquarters": df.Headquarters,
        "Website": df.Website,
        "Employees": df.Employees,
        "Stock_Symbol": df.Stock_Symbol,
        "Ticker": df.Ticker,
        "Revenue": df.Revenue,
        "Phone": df.Phone,
        "Siccode": df.Siccode,
        "Naicscode": df.Naicscode,
        "Description": df.Description,
        "Industry": df.Industry,
        "Company_Logo_URL":df.Company_Logo_URL
    }
    db.company.update_one(
        {"Company_ID": df.Company_ID},
        {'$set': data},
        upsert=True
    )
    global count
    count+=1
    print(count)


info_dataframe.writeStream.foreach(write_mongo_row).start().awaitTermination()


# def write_mongo_row(df, epoch_id): 
#     print("DATA IS UPLOADING............")
#     df.write.format("mongo").mode("overwrite").option("database","new_data").option("collection", "company").save()
#     print("DATA HAS BEEN UPLOADED YOU CAN CLOSE THE TERMINAL")
#     pass


# info_dataframe.writeStream.foreachBatch(write_mongo_row).start().awaitTermination()