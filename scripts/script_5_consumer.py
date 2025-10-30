from log_mlflow import load_production_model
from kafka import KafkaConsumer
import json
import psycopg2
import pandas as pd
import os 

tracking_uri = "http://mlflow:5001"
model_name = "fraud_detection_test"
table_name = "infered_transactions"

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

# Kafka Consumer setup
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fraud-detectors',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

batch = []
batch_size = 1000

print("Consumer running...")

for message in consumer:
    event = message.value
    batch.append(event)

    if len(batch) >= batch_size:
        print(f"Processing batch of {len(batch)} transactions...")

        # Convert to DataFrame
        df = pd.DataFrame(batch)

        # Model inference
        predictions = model(df)

        # Append predictions
        df["prediction"] = predictions

        # Bulk insert into PostgreSQL
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO processed_transactions
                (transaction_id, user_id, amount, timestamp, location, device_type, prediction)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING;
            """, (
                row["transaction_id"],
                row["user_id"],
                row["amount"],
                row["timestamp"],
                row["location"],
                row["device_type"],
                row["prediction"]
            ))

        conn.commit()
        print(f"✅ Inserted {len(batch)} records into processed_transactions.")

        batch.clear()