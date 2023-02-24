import time
import os
from pyspark.sql import SparkSession
from convert_to_csv import convert
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")

spark = SparkSession.builder.appName("Upload to Mongodb")\
            .master("local[*]")\
            .config("spark.mongodb.input.uri", MONGO_URL) \
            .config("spark.mongodb.output.uri", MONGO_URL) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

count=0
spark.conf.set("spark.sql.streaming.schemaInference","true")

# convert()
dataStream  = spark.readStream\
    .format("csv")\
    .option("inferSchema","true")\
    .option("header", "true")\
    .option("maxFilesPerTrigger", 1)\
    .load("./data")

dataStream.printSchema()
print("Streaming DataFrame : ", dataStream.isStreaming)


def write_mongo_row(df): 
    cluster = MongoClient(MONGO_URL)
    db = cluster["load_data"]
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
    db.country.update_one(
        {"Company_ID": df.Company_ID},
        {'$set': data},
        upsert=True
    )
    global count
    count+=1
    print(count,",",end=" ")


query = dataStream.writeStream.foreach(write_mongo_row)\
        .start()

query.awaitTermination()

