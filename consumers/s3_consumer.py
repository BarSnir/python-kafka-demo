import os
import json
import logging as log
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer
import boto3
from datetime import datetime
import pytz


def full_flow_s3_consumer():
    log.info("S3 consumer is up.")
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
    group_id = 's3-consumers'
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
                print("Consumer for S3: ", record_key)
                upload_to_s3(record_value, s3_session)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def upload_to_s3(payload, s3_session):
    bucket = "kafka-ad-history-poc"
    dict_payload = json.loads(payload.decode('utf-8'))
    root_dir=dict_payload.get('token')
    timestamp = get_il_time()
    path = f"{root_dir}/{timestamp}.json"
    s3_object = s3_session.Object(bucket, path)
    s3_object.put(Body=payload)

def get_il_time():
    tz = pytz.timezone('Israel')
    ct = datetime.now(tz=tz)
    return ct.isoformat()


full_flow_s3_consumer()    
