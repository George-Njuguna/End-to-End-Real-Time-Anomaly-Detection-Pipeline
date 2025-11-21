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
table_name_1 = "Transactions"
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
    enable_auto_commit=False,
    group_id='fraud-detectors',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    max_poll_records=2000,
    max_poll_interval_ms=900000,  
    session_timeout_ms=30000,
    heartbeat_interval_ms=10000
)

batch = []
batch_size = 300

no_msg_count = 0
max_idle_polls = 30

print("Consumer running...")

# creating the table if does not exist
create_prediction_table(conn, table_name) 

while True:
    msg_pack = consumer.poll(timeout_ms=5000)

    # If no messages were returned this poll
    if not msg_pack:
        no_msg_count += 1

        # If we have seen no messages for many polls → assume no more new data
        if no_msg_count >= max_idle_polls:
            print("✅ No new messages for a while — assuming producer finished. Finalizing any remaining batch...")

            if batch:
                df = pd.DataFrame(batch)
                column1 = df['time_seconds']
                column2 = df["processed_at"]

                #dropping the processed at 
                df = df.drop("processed_at", axis=1)

                 # loading data in transaction for later modelling
                load_data(conn, df, table_name_1)

                test_data, y = split_func(df)
                predictions = model.predict(test_data)
                probabilities = model.predict_proba(test_data)[:, 1] * 100

                test_data['fraud'] = y
                test_data['time_seconds'] = column1
                test_data['processed_at'] = column2
                test_data['prediction'] = predictions
                test_data['probability'] = probabilities
                
                 # loading for inference
                load_data(conn, test_data, table_name)
                consumer.commit()
                batch.clear()

            print("✅ Consumer finished processing all messages. Stopping gracefully.")
            break

        continue

    # reset idle counter
    no_msg_count = 0

    # Add received messages to batch
    for tp, messages in msg_pack.items():
        for message in messages:
            event = message.value
            batch.append(event)

    # Flush batch 
    if len(batch) >= batch_size or (datetime.now() - last_flush) > flush_interval:
        print(f" Processing batch of {len(batch)} transactions...")

        df = pd.DataFrame(batch)
        column1 = df['time_seconds']
        column2 = df["processed_at"]

         # dropping processed data
        df = df.drop("processed_at", axis=1)

         # loading data in transaction for later modelling
        load_data(conn, df, table_name_1)

        test_data, y = split_func(df)
        predictions = model.predict(test_data)
        probabilities = model.predict_proba(test_data)[:, 1] * 100

        test_data['fraud'] = y
        test_data['time_seconds'] = column1
        test_data['processed_at'] = column2
        test_data['prediction'] = predictions
        test_data['probability'] = probabilities

         # loading for inference
        load_data(conn, test_data, table_name)
        consumer.commit()

        batch.clear()
        last_flush = datetime.now()

print("✅ Kafka consumer fully stopped.")