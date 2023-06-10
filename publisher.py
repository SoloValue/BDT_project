from kafka import KafkaProducer
import json
import numpy as np

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers = ["localhost:29092"],
                         value_serializer = json_serializer)
    
if __name__ == "__main__":
    json_to_send = {
        "name": "Teo",
        "hope": "not really",
        "will it work": "plz I need it",
        "how much in a scale to 1 to 10": np.random.randint(1,12)
    }
    #print(json_to_send)
    producer.send("dump_try", value=json_to_send)
    producer.flush()