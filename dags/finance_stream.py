#Import important Modules
import random
import time
import sys
import os
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import  PythonOperator
import logging
from confluent_kafka import Producer

# Add root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from jobs.config import configuration
from faker import Faker

#initialise Faker
fake = Faker()

#Specify the Kafka Topic
KAFKA_TOPIC = 'financial_data'

#Currency Type
currencies = ['USD', 'EUR', 'GBP', 'INR', 'JPY']
#Transaction Types
transaction_types = ['deposit', 'withdrawal', 'transfer', 'payment']

#This function generates the financial data using Faker
def generate_financial_data():
    data = {}
    data['transaction_id'] = str(uuid.uuid4())
    data['timestamp'] = datetime.utcnow().isoformat()
    data['sender_id'] = fake.uuid4()
    data['receiver_id'] = fake.uuid4()
    data['amount'] = str(round(random.uniform(10, 10000), 2))
    data['currency'] = random.choice(currencies)
    data['longitude'] = str(fake.longitude())
    data['latitude'] = str(fake.latitude())
    data['device_id'] = fake.uuid4()
    data['transaction_type'] = random.choice(transaction_types)

    return data

#The function grabs the config information from config.py
def get_configs():
    config = {}
    config['bootstrap.servers'] = configuration['bootstrap.servers']
    config['sasl.username'] = configuration['sasl.username']
    config['sasl.password'] = configuration['sasl.password']
    config['security.protocol'] = configuration['security.protocol']
    config['sasl.mechanisms'] = configuration['sasl.mechanisms']
    config['acks'] = configuration['acks']
    return config

#Delivery callback
def delivery_callback(err, msg):
    if err:
        print("Error: Message delivery failed: {}".format(err))
    else:
        print(f"Produced event to topic {KAFKA_TOPIC}")

#This functions streams the created financial data to Kafka
#The data is converted to json and produced to kafka
def stream_to_kafka():
    import json

    curr_time = time.time()
    while True:
        if time.time() > curr_time+60:
            break
        try:
            data = generate_financial_data()
            config_data = get_configs()
            producer = Producer(config_data)
            producer.produce(KAFKA_TOPIC, json.dumps(data).encode('utf-8'), callback=delivery_callback)

            #Block until the messages are sent
            producer.poll(10000)
            producer.flush()
            logging.info(f"Record Stream to {KAFKA_TOPIC}")
        except Exception as e:
            logging.error(f"Data not successfully sent to {KAFKA_TOPIC}: {e}")

#Initialising the DAG argument
default_args = {
        'owner': 'jaxhacker',
        'start_date': datetime(2025, 5, 12, 1, 40)
    }

#PythonOperator is used and the 'stream_to_kafka' function us called
with DAG(
        "streaming_financial_data_to_kafka",
        default_args=default_args,
        schedule='@daily',
        catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_financial_data_to_kafka',
        python_callable=stream_to_kafka
    )
