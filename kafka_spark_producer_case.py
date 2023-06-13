import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
# from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "/Users/Adithya/Desktop/iNeuron/Spark/Assignment/Case.csv"
columns = ['case_id', 'province', 'city', 'group', 'infection_case', 'confirmed', 'latitude', 'longitude']

API_KEY = 'GHVGAQVJ3RSWJEKA'
ENDPOINT_SCHEMA_URL  = 'https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud'
API_SECRET_KEY = 'g1rJY1gedecr723jMlYJnNEOtAVinFdcBnq+lsqpAVhJ5GWU3lXxrDP1gztETSmM'
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MECHANISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'GHV5DZXEKMAQQGGQ'
SCHEMA_REGISTRY_API_SECRET = '91zPqTVUYyvF0wqjR7p4Z4np4k8x7nWGMVJhYPhqhkcttiUrI45I5YMbnW0r4NbQ'


def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MECHANISM,
                 # Set to SASL_SSL to enable TLS support.
                 #  'security.protocol': 'SASL_PLAINTEXT'}
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,

            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

            }


class Order:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

        self.record = record

    @staticmethod
    def dict_to_order(data: dict, ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"


def get_order_instance(file_path):
    df = pd.read_csv(file_path)
    orders: List[Order] = []
    for data in df.values:
        print(data)
        order = Order(dict(zip(columns, data)))
        orders.append(order)
        yield order


def case_to_dict(order: Order, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
        :param order:
    """

    # User._address must not be serialized; omit from dict
    return order.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):

    schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "case_id": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "province": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "city": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "group": {
      "description": "The type(v) type is used.",
      "type": "boolean"
    },
    "infection_case": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "confirmed": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "latitude": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "longitude": {
      "description": "The type(v) type is used.",
      "type": "string"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
    # To Get (Latest) Schema to be read from Kafka
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_str = schema_registry_client.get_latest_version('topic_case-1-value').schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, case_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    # while True:
    # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for order in get_order_instance(file_path=FILE_PATH):
            print(order)
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4()), case_to_dict),
                             value=json_serializer(order, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()


main("topic_case-1")
