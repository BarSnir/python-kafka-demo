import os
import json
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Producer
import time

delivered_records = 0

def full_flow_producer():
    load_envs()
    producer = get_producer()
    payload = get_payload()
    send_messages(producer, payload)

def load_envs():
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)

def get_producer():
    producer_config = get_producer_config()
    return  Producer(producer_config)

def get_producer_config():
    host = os.getenv('KAFKA_HOST')
    kafka_username = os.getenv('KAFKA_USER')
    kafka_password = os.getenv('KAFKA_PASS')
    return {
        'bootstrap.servers': host, 
        'security.protocol': 'SASL_SSL', 
        'sasl.mechanisms': 'PLAIN', 
        'sasl.username': kafka_username, 
        'sasl.password': kafka_password
    }

def serve_ack_status(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

def get_payload():
    with open('poc.json') as f:
        return json.load(f)

def send_messages(producer, payload):
    topic = 'test_topic'
    for item in payload:
        token = item.get('token')
        record_key = token
        record_value = json.dumps(item)
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=serve_ack_status)
        producer.poll(0)
        producer.flush()
        time.sleep(30)


full_flow_producer()