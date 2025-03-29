import psycopg2
import json
from kafka import KafkaProducer

# ✅ PostgreSQL Connection
conn = psycopg2.connect(
    host="postgres",
    database="airflow",
    user="airflow",
    password="airflow"
)

# ✅ Use a Server-Side Cursor (Prevents Memory Overload)
cursor = conn.cursor(name="pg_cursor")  

# ✅ Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    linger_ms=500,
    batch_size=32768,
)

# ✅ Read Data in Batches
BATCH_SIZE = 50000  
cursor.itersize = BATCH_SIZE
cursor.execute("SELECT * FROM cricket_data_partition;")

total_records = 0
while True:
    batch = cursor.fetchmany(BATCH_SIZE)

    if not batch:
        break  

    for row in batch:
        record_dict = dict(zip([desc[0] for desc in cursor.description], row))
        producer.send("cricket_stream", value=record_dict)

    total_records += len(batch)
    print(f"✅ Processed {total_records} records so far...")

# ✅ Close the Cursor *Before* Committing
cursor.close()
conn.commit()

print(f"✅ Successfully pushed {total_records} records to Kafka!")

# ✅ Cleanup
producer.flush()
producer.close()
conn.close()