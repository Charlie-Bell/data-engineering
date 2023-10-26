from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv
import s3fs

load_dotenv()

# Private IPs connected through VPN. Can have multiple bootstrap_servers/nodes/brokers
SERVER_1_IP = os.environ.get('SERVER_1_IP')
bootstrap_servers = [f'{SERVER_1_IP}:9092']
topic = 'my-topic'
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET')

s3 = s3fs.S3FileSystem(
    key=ACCESS_KEY,
    secret=SECRET_KEY
)

for i, record in enumerate(consumer):
    print(record)
    with s3.open(f"{S3_BUCKET}outputs/stock_market_{i}.json", 'w') as f:
        json.dump(record.value, f)