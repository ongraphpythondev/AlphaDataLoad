from json import loads  
from kafka import KafkaConsumer  
from pymongo import MongoClient
import os
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import time

load_dotenv()

ec = Elasticsearch("http://localhost:9200")

MONGO_URL = os.getenv("MONGO_URL")
cluster = MongoClient(MONGO_URL)
db = cluster["company_db"]

my_consumer = KafkaConsumer(  
    'company_data_new',  
     bootstrap_servers = ['localhost : 9092'],  
     auto_offset_reset = 'earliest',  
    #  enable_auto_commit = True,  
     group_id = 'my-group-a',  
     value_deserializer = lambda x : loads(x.decode('utf-8'))  
     )  

for message in my_consumer:  
    message = message.value  
    print(message)
    message['Company_ID'] = int(message['Company_ID'])
    message['Id'] = int(message['Id'])
    if 'Employees' in message.keys():
        try:
            message['Employees'] = int(''.join(message['Employees'].split(',')))
        except:
            pass
    if 'Revenue' in message.keys():
        if message['Revenue']:
            message['Revenue'] = int(''.join(message['Revenue'].split(",")))
    if 'Siccode' in message.keys():
        message['Siccode'] = int(''.join(message['Siccode'].split(",")))
    if 'Naicscode' in message.keys():
        message['Naicscode'] = int(''.join(message['Naicscode'].split(',')))
    print(message)
    db.company.update_one(
        {"Company_ID": message['Company_ID']},
        {'$set': message},
        upsert=True
    )
    ec.index(index="company",document=message)
    print("Uploaded")
    # time.sleep(5)
