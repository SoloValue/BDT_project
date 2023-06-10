from kafka import KafkaConsumer
import json

def json_deserializer(data):
  return json.loads(data.decode('utf-8'))

consumer = KafkaConsumer(
 bootstrap_servers='localhost:29092',
 value_deserializer = json_deserializer,
 auto_offset_reset='earliest'
)

consumer.subscribe(topics='dump_try')
print("Connected to broker")
for message in consumer:
  print ("%d:%d: msg=%s" % (message.partition,
                          message.offset,
                          message.value))