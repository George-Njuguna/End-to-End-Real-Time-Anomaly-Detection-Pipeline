from pipelines import fetch_batch_data
from kafka import KafkaProducer
import psycopg2
import os 
import json
import time 


table_name = 'streaming_data'
table2 = "streaming_data_test"
msg_count= 0

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
    bootstrap_servers=['kafka-1:9092','kafka-2:9092','kafka-3:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',         
    linger_ms=5,          
    batch_size=32768,     
    compression_type='lz4' 
)


if __name__ == "__main__":
    print("Producer started...")
    while True:
        transactions = fetch_batch_data(table2, 1000, conn) 
        if not transactions:
            print("✅ No more transactions left to stream. Stopping producer.")
            break 

        for txn in transactions:
            producer.send("transactions", txn)
            msg_count += 1
            time.sleep(0.3)
        print(f'Produced {msg_count} messages...')

    producer.flush()
    producer.close()
    conn.close()




