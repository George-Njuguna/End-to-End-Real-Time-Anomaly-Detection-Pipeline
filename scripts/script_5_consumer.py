from log_mlflow import load_production_model
from functions import split_func, create_prediction_table, import_data, load_data
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import pendulum 
import json
import psycopg2
import pandas as pd
import os 

tracking_uri = "http://mlflow:5001"
model_name = "fraud_detection_test"
table_name = "infered_transactions"
model_name = "fraud_detection_test"

 # Connecting to database 
conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PW'),
    host = os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT')
)

model, run_id, version = load_production_model(model_name, tracking_uri)

cursor = conn.cursor()

last_flush = datetime.now()
flush_interval = timedelta(seconds=30)

# Kafka Consumer setup
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fraud-detectors',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

batch = []
batch_size = 1000

no_msg_count = 0
max_empty_polls = 10 

print("Consumer running...")

# creating the table if does not exist
create_prediction_table(conn, table_name) 

while True:
    msg_pack = consumer.poll(timeout_ms=1000)

    if not msg_pack:
        no_msg_count += 1
        if no_msg_count >= max_empty_polls:
            print("No new messages — stopping consumer.")
            break
        continue

    no_msg_count = 0

    for tp, messages in msg_pack.items():
        for message in messages:
            event = message.value
            event["processed_at"] = pendulum.now("Africa/Nairobi").format("YYYY-MM-DD HH:mm:ss")
            batch.append(event)

    if len(batch) >= batch_size or datetime.now() - last_flush > flush_interval:
        print(f"Processing batch of {len(batch)} transactions...")

        df = pd.DataFrame(batch)
        column = df['time_seconds']

        test_data, y = split_func(df)
        predictions = model.predict(test_data)
        probabilities = model.predict_proba(test_data)[:, 1] * 100

        test_data['fraud'] = y
        test_data['time_seconds'] = column
        test_data['prediction'] = predictions
        test_data['probability'] = probabilities

        load_data(conn, test_data, table_name)

        batch.clear()
        last_flush = datetime.now()
