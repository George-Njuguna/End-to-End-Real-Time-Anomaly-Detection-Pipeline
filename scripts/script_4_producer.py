from pipelines import fetch_batch_data
from functions import load_last_transaction_id,update_last_transaction_id, update_batch_status, load_batch_data
from kafka import KafkaProducer
import psycopg2
import os 
import json
import time 
import random as rd

table_name = 'streaming_data'
last_id_table = 'transaction_id_table'
batch_table = 'batch_table'
table2 = "streaming_data_test"
msg_count= 0
msg_list = [3000,4000,5000,6000,8000,7000]

 # Connecting to database 
conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PW'),
    host = os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT')
)

last_id = load_last_transaction_id(conn, last_id_table)
batch, date = load_batch_data(conn, batch_table)

# Kafka Producer 
producer = KafkaProducer(
    bootstrap_servers=['kafka-1:9092','kafka-2:9092','kafka-3:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  
    enable_idempotence=True,       
    linger_ms=5,          
    batch_size=32768,     
    compression_type='lz4' 
)

print("Producer started...")

while True:
    transactions = fetch_batch_data(table2, batch, conn, last_id)

    if not transactions:
        print("✅ No more transactions left to stream. Stopping producer.")
        break

    batches += len(transactions)
    print(f"....Imported {batches} data.....")

    new_last_id = last_id

    for txn in transactions:
        producer.send("transactions", txn)
        msg_count += 1
        new_last_id = txn["transaction_id"]

    #  flushing the batch
    producer.flush()
    last_id = new_last_id

    print(f"Produced {msg_count} messages so far...")

update_last_transaction_id(conn, last_id_table, last_id)
producer.flush()
producer.close()
conn.close()


def main():
    print(".....END OF THE PRODUCER....")
    
if __name__ == '__main__':
    main()





