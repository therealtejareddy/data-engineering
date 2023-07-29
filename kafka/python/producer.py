from kafka import KafkaProducer
from json import dumps
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
producer.send('test1',value={'name':'teja'})