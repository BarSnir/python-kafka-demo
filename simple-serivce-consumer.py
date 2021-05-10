import os
import json
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer

def full_flow_producer():
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
    group_id = 'simple-service-A'
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
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                print(
                    "Consumed record with key {} and value {}".format(
                        record_key, 
                        record_value
                    )
                )
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

full_flow_producer()    
