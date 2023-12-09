from kafka import KafkaConsumer

consumer = KafkaConsumer("third_topic",bootstrap_servers="localhost:9092",auto_offset_reset='earliest')

for msg in consumer:
    if msg != None or not msg:
        print(msg.value)