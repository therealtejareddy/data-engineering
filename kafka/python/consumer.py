from kafka import KafkaConsumer
from json import loads
consumer = KafkaConsumer("test1",bootstrap_servers=['127.0.0.1:9092'], value_deserializer=lambda x: loads(x).encode('utf-8'))
for i in consumer:
    print(i.value)