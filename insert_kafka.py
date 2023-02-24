from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct
from convert_to_csv import convert


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
spark = SparkSession.builder.appName("Insert to kafka")\
            .master("local[*]")\
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
            .getOrCreate()

spark.conf.set("spark.sql.streaming.schemaInference","true")
convert()
readData = spark.readStream\
            .format("csv")\
            .option("inferSchema","true")\
            .option("header", "true")\
            .option("maxFilesPerTrigger", 1)\
            .load("./data")


readData.printSchema()
print("Streaming DataFrame : ", readData.isStreaming)
readData = readData.select([col(c).cast("string") for c in readData.columns])
readData.printSchema()
readData = readData.withColumn("value", to_json(struct("*")).cast("string"),)


def write_mongo_row(df, epoch_id):
    print("DATA IS UPLOADING............")
    df.write.format("kafka").mode("overwrite").option("topic","country_topic")\
            .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVER)\
            .save()
    print("DATA HAS BEEN UPLOADED YOU CAN CLOSE THE TERMINAL")
    pass


print("DATA IS UPLOADING............")
readData.select("value").writeStream\
        .trigger(processingTime="10 seconds")\
        .outputMode("append")\
        .format("kafka")\
        .option("topic","new_data")\
        .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVER)\
        .option("checkpointLocation", "/tmp/data3")\
        .start()\
        .awaitTermination()

# readData.select("value").writeStream.foreachBatch(write_mongo_row).start().awaitTermination()
#         .trigger(processingTime="10 seconds")\
#         .outputMode("append")\
#         .format("kafka")\
#         .option("topic","country_topic")\
#         .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVER)\
#         .option("checkpointLocation", "/tmp/checkpoint")\
#         .start()\
#         .awaitTermination()