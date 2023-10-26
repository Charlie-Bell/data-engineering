import pandas as pd
from kafka import KafkaProducer
from time import sleep
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Private IPs connected through VPN. Can have multiple bootstrap_servers/nodes/brokers
SERVER_1_IP = os.environ.get('SERVER_1_IP')
bootstrap_servers = [f'{SERVER_1_IP}:9092']
topic = 'my-topic'

# Create producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x:
                         json.dumps(x).encode('utf-8'))

# Typically this would be live streamed data.
df = pd.read_csv("data/indexProcessed.csv")

# Send sample from dataset and sleep for 1 second to imitate live streaming
for i in df.index:
    value = df.iloc[[i]].to_dict(orient='records')[0]
    producer.send(topic=topic, value=value)
    sleep(1)