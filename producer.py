import os
import json
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Producer

delivered_records = 0

def full_flow_producer():
    load_envs()
    producer = get_producer()
    num_of_messages = 10
    send_messages(producer, num_of_messages)
    producer.flush()

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

def send_messages(producer, num_of_messages):
    topic = 'test_topic'
    for n in range(num_of_messages):
        record_key = "alice"
        record_value = json.dumps({'count': n})
        partition_key = f"{record_key}_{n}"
        print("Producing record: {}\t{}".format(partition_key, record_value))
        producer.produce(topic, key=partition_key, value=record_value, on_delivery=serve_ack_status)
        producer.poll(0)

full_flow_producer()