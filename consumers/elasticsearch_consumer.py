import os
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer
import boto3
import logging as log



def full_flow_elasticsearch_consumer():
    print("Elasticsearch consumer is up.")
    load_envs()
    consumer = get_kafka_consumer()
    subscribe(consumer)
    s3_session = get_s3_session()
    consume(consumer, s3_session)

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

def get_s3_session():
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secrest_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secrest_access_key,
    )
    return session.resource('s3')


def consume(consumer, s3_session):
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
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

full_flow_elasticsearch_consumer()