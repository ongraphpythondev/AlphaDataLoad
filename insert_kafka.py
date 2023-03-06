from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct,udf, split, element_at
from pyspark.sql.types import StringType
# from convert_to_csv import convert


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
spark = SparkSession.builder.appName("Insert to kafka")\
            .master("local[*]")\
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
            .getOrCreate()

spark.conf.set("spark.sql.streaming.schemaInference","true")
# convert()
readData = spark.readStream\
            .format("csv")\
            .option("inferSchema","true")\
            .option("header", "true")\
            .option("maxFilesPerTrigger", 1)\
            .load("./data")

def convert(s):
  if "Million" in s:
    return ''.join(filter(str.isdigit,s))+"000000"
  if "Billion" in s:
    return ''.join(filter(str.isdigit,s))+"000000000"
  return ''.join(filter(str.isdigit, s))


def write_string(s):
  special = "!#$%'()*+,-./:;<=>?@[\]^_`{|}~±™⁀–©≈°Ø•"
  return ''.join(i for i in s if i not in special)

def employee(s):
  return ''.join(filter(str.isdigit, s))


employe_df = udf(lambda x: employee(x), StringType())
upperCaseUDF = udf(lambda x:convert(x),StringType()) 
stringdf = udf(lambda x: write_string(x), StringType())



readData.printSchema()
print("Streaming DataFrame : ", readData.isStreaming)
readData = readData.fillna(value="0", subset=["Employees","Revenue","Siccode", 'Naicscode'])
readData = readData.fillna(value="", subset=["Company_Name"])
readData = readData.withColumn("Revenue", upperCaseUDF(col('Revenue')))
readData = readData.withColumn("Company_Name", stringdf(col('Company_Name')))
readData = readData.withColumn("Employees", employe_df(col('Employees')))
readData = readData.withColumn("Naicscode", upperCaseUDF(col('Naicscode')))
readData = readData.withColumn("Siccode", upperCaseUDF(col('Siccode')))

readData = readData.withColumn("Country",element_at(split(readData["Headquarters"], ', '), -1))\
  .withColumn("City",element_at(split(readData["Headquarters"], ', '), -3))

readData = readData.select([col(c).cast("string") for c in readData.columns])
readData.printSchema()
readData = readData.withColumn("value", to_json(struct("*")).cast("string"),)

print("DATA IS UPLOADING............")
readData.select("value").writeStream\
        .trigger(processingTime="10 seconds")\
        .outputMode("update")\
        .format("kafka")\
        .option("topic","company_data_new")\
        .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVER)\
        .option("checkpointLocation", "/tmp/data3")\
        .trigger(processingTime='2 seconds')\
        .start()\
        .awaitTermination()
