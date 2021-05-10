import os
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
import json

def full_flow_elasticsearch_consumer():
    print("Elasticsearch consumer is up.")
    load_envs()
    consumer = get_kafka_consumer()
    subscribe(consumer)
    es_client = get_es_client()
    consume(consumer, es_client)

def load_envs():
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)

def get_kafka_consumer():
    consumer_config = get_kafka_consumer_config()
    return Consumer(consumer_config)

def get_kafka_consumer_config():
    host = os.getenv('KAFKA_HOST')
    kafka_username = os.getenv('KAFKA_USER')
    kafka_password = os.getenv('KAFKA_PASS')
    group_id = 'elasticsearch-consumers'
    protocol = 'SASL_SSL'
    mechanisms =  'PLAIN'
    auto_offset_reset = 'earliest'
    return {
        'bootstrap.servers': host,
        'security.protocol': protocol,
        'sasl.mechanisms': mechanisms,
        'sasl.username': kafka_username,
        'sasl.password': kafka_password,
        'group.id': group_id,
        'auto.offset.reset': auto_offset_reset,
    }

def subscribe(consumer):
    topic = 'test_topic'
    consumer.subscribe([topic])

def get_es_client():
    host_string = "{protocol}://{user}:{password}@{host}".format(
            protocol=os.environ.get("ELASTICSEARCH_PROTOCOL"),
            user=os.environ.get("ELASTICSEARCH_USER"),
            password=os.environ.get("ELASTICSEARCH_PASSWORD"),
            host=os.environ.get("ELASTICSEARCH_URL"),
    )
    return Elasticsearch(host_string)


def consume(consumer, es_client):
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                print("Consumer for Elasticsearch:", record_key)
                index_in_es(es_client, record_value)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def index_in_es(es_client, value):
    index = "kafka_streams_poc"
    body = json.loads(value.decode('utf-8'))
    es_client.index(
        index,
        body,
    )
    print("Indexed in Elasticsearch")

full_flow_elasticsearch_consumer()