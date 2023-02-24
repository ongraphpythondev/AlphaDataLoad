from json import loads  
from kafka import KafkaConsumer  
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")
cluster = MongoClient(MONGO_URL)
db = cluster["new_data"]

my_consumer = KafkaConsumer(  
    'new_data',  
     bootstrap_servers = ['localhost : 9092'],  
     auto_offset_reset = 'earliest',  
    #  enable_auto_commit = True,  
     group_id = 'my-group-a',  
     value_deserializer = lambda x : loads(x.decode('utf-8'))  
     )  

for message in my_consumer:  
    message = message.value  
    print(message)
    db.company.update_one(
        {"Company_ID": message['Company_ID']},
        {'$set': message},
        upsert=True
    )
    print("Uploaded")
