import argparse
import pymongo
from pymongo import MongoClient
import json
import pandas as pd

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

# MongoDB Atlas connection details
mongo_username = ""
mongo_password = ""
mongo_clustername = "Kafka-Mongo"
mongo_database = "kafka_mongo_covid_ds"
mongo_collection1 = "case_1"
mongo_collection2 = "region_1"
mongo_collection3 = "time_province_1"
#mongo_uri = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_clustername}.mongodb.net/{mongo_database}?retryWrites=true&w=majority"
mongo_uri = f"mongodb+srv://mongodb:mongodb@kafka-mongo.cgjosxp.mongodb.net/?retryWrites=true&w=majority"



API_KEY = ''
ENDPOINT_SCHEMA_URL  = 'https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud'
API_SECRET_KEY = ''
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MECHANISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = ''
SCHEMA_REGISTRY_API_SECRET = ''


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MECHANISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Order:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"

def main(topic1,topic2,topic3):

    # To Get (Latest) Schema to be read from Kafka
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_str_1 = schema_registry_client.get_latest_version('topic_case-1-value').schema.schema_str
    schema_str_2 = schema_registry_client.get_latest_version('topic_region-1-value').schema.schema_str
    schema_str_3 = schema_registry_client.get_latest_version('topic_timeprovince-1-value').schema.schema_str

    json_deserializer1 = JSONDeserializer(schema_str_1,from_dict=Order.dict_to_order)
    json_deserializer2 = JSONDeserializer(schema_str_2,from_dict=Order.dict_to_order)
    json_deserializer3 = JSONDeserializer(schema_str_3,from_dict=Order.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "latest"
                     })

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic1, topic2, topic3])
    messages = []

    # Set up MongoDB Atlas client
    client = MongoClient(mongo_uri)
    db = client[mongo_database]
    collection1 = db[mongo_collection1]
    collection2 = db[mongo_collection2]
    collection3 = db[mongo_collection3]


    count1 = 0
    count2 = 0
    count3 = 0

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                if len(messages) != 0:
                    messages = []
                continue

            messages = []
            
            topic = msg.topic()
            order1 = None
            order2 = None
            order3 = None
            if topic == topic1:
                order1 = json_deserializer1(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            elif topic == topic2:
                order2 = json_deserializer2(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            elif topic == topic3:
                order3 = json_deserializer3(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if order1 is not None:
                count1=count1+1

                # print("11111User record {}: order: {}\n"
                #      .format(msg.key(), order1))
                messages.append(order1.record)
                df = pd.DataFrame.from_records(messages)
                json_str = df.to_dict(orient='records')
                collection1.insert_many(json_str)
                print(f"Record written to MongoDB Atlas: {json_str}")

            if order2 is not None:
                count2=count2+1

                # print("22222User record {}: order: {}\n"
                #      .format(msg.key(), order2))
                messages.append(order2.record)
                df = pd.DataFrame.from_records(messages)
                json_str = df.to_dict(orient='records')
                collection2.insert_many(json_str)
                print(f"Record written to MongoDB Atlas: {json_str}")

            if order3 is not None:
                count3=count3+1

                # print("33333User record {}: order: {}\n"
                #      .format(msg.key(), order3))
                messages.append(order3.record)
                df = pd.DataFrame.from_records(messages)
                json_str = df.to_dict(orient='records')
                collection3.insert_many(json_str)
                print(f"Record written to MongoDB Atlas: {json_str}")

            # for message in consumer:
            #     print("############### RAW MSG #################")
            #     print(message)
            #     topic = message.topic.split(".")
            #     tname = topic.pop()
            #     print(tname)
            #     coll = db[tname]
            #     cdc_coll = db["cdc_"+tname]

            

        except KeyboardInterrupt:
            print(count1)
            print(count2)
            print(count3)
            break

    consumer.close()

main("topic_case-1","topic_region-1","topic_timeprovince-1")
