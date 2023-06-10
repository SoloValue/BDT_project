import json

def serializer(data):
  '''
  Json serializer for the kafka producer (utf-8)
  '''
  return json.dumps(data).encode("utf-8")

def deserializer(data):
  '''
  Json serializer for the kafka producer (utf-8)
  '''
  return json.loads(data.decode('utf-8'))