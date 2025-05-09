from kafka import KafkaConsumer 
import pandas as pd
from sqlalchemy import create_engine
import time

# Kafka consumer configuration
consumer = KafkaConsumer(
    'test-topic4',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

# SQL Server connection
DATABASE_URL = "mssql+pyodbc:///?odbc_connect=" + \
    "Driver={ODBC Driver 17 for SQL Server};" + \
    "Server=Toka;" + \
    "Database=Kafka_project;" + \
    "Trusted_Connection=yes;"
engine = create_engine(DATABASE_URL)
table_name = 'server_matric'  

# Buffer to collect messages
message_buffer = []
start_time = time.time()
batch_interval = 120  # 2 minutes
batch_size = 1000

print("Starting consumer...")

for message in consumer:
    try:
        parts = message.value.strip().split(',')
        parsed_data = {}
        for item in parts:
            key, value = item.split(':')
            parsed_data[key.strip()] = int(value.strip())

        message_buffer.append(parsed_data)

    except Exception as e:
        print("Failed to parse message:", message.value)
        print(e)

    # Save to DB every 2 minutes or 1000 messages
    if len(message_buffer) >= batch_size or (time.time() - start_time) >= batch_interval:
        if message_buffer:
            df = pd.DataFrame(message_buffer)
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f"Inserted {len(message_buffer)} rows into {table_name}")
            message_buffer.clear()
            start_time = time.time()
