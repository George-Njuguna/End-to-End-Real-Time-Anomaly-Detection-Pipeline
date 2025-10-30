from pipelines import fetch_batch_data
from kafka import KafkaProducer
import psycopg2
import os 
import json
import time 


table_name = 'streaming_data'



 # Connecting to database 
conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PW'),
    host = os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT')
)

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

if __name__ == "__main__":
    print("Producer started...")
    while True:
        transactions = fetch_batch_data(table_name, 1000, conn) 
        if not transactions:
            print("✅ No more transactions left to stream. Stopping producer.")
            break 

        for txn in transactions:
            producer.send("transactions", txn)
            print(f"Produced: {txn}")
            time.sleep(0.5)

    producer.flush()
    producer.close()
    conn.close()




