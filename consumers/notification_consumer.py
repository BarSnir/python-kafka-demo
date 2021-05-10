import os
import json
import logging as log
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer
import boto3
from datetime import datetime
import pytz


def full_flow_notification_consumer():
    print("Notification consumer is up.")
    load_envs()
    consumer = get_kafka_consumer()
    subscribe(consumer)
    consume(consumer)

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
    group_id = 'notification-consumers'
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

def consume(consumer):
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
                action = get_action(record_value)
                print("Consumer for notifications: ", record_key)
                print(f"Notify that user did {action} action")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def get_action(value):
    payload = json.loads(value.decode('utf-8'))
    return payload.get('action')

def get_il_time():
    tz = pytz.timezone('Israel')
    ct = datetime.now(tz=tz)
    return ct.isoformat()


full_flow_notification_consumer()    
