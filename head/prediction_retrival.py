import pymongo
from datetime import datetime

def prediction_retrival(connection_string, database_name, id_location, request_time, max_age=30):
  mongo_client = pymongo.MongoClient(connection_string)
  mongo_collection = mongo_client[database_name]["predictions"]

  last_prediction = mongo_collection.find({"id_location": id_location}).sort("request_time", pymongo.DESCENDING).limit(1)
  try:
    last_prediction = last_prediction.next()
  except:
    return -1
  pred_time = datetime.fromisoformat(last_prediction["request_time"])
  delta_time = abs((request_time-pred_time).total_seconds())
  if delta_time < max_age*60:
    return last_prediction
  else:
    return -1

if __name__ == "__main__":
  connection_string="mongodb://root:psw@localhost:27017/"
  last_prediction = prediction_retrival(connection_string, "output", 1, datetime.now())
  print(last_prediction)