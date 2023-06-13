#FUNCTIONS TO GET DATA FROM APIS
# LIBRARIES
import requests
import json
import pymongo 
import datetime
import yaml

# CONNECT TO MONGO ATLAS DATABASE 
CONFIG_PATH = "./config/config.yaml"
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)
CONNECTION_STRING = config["mongodb"]["atlas"]["connection_string"]
myclient = pymongo.MongoClient(CONNECTION_STRING)
mydb = myclient["mydatabase"]

# REAL TIME REQUESTS -------------------------------------
def rt_tomtom_request(lat, long_):

    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json?point={lat}%2C{long_}&unit=KMPH&openLr=false&key=FShxCHa2f5LlOL479fZRSYJJuTGBe3J4"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data_json = response.json()
    result = data_json["flowSegmentData"]
    
    return result 

def rt_air_request(lat, long_):
    url = f"http://api.airvisual.com/v2/nearest_city?lat={lat}&lon={long_}&key=a2abc955-cedb-4d19-ab75-1d3346eee4b6&=fbfab6a9659cda24ae3aa9d35cba5d070307c29e"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data_json = response.json()
    result = data_json["data"]

    return result

# INSERT DATA FROM REQUESTS --------------------------------
def insert_docs(tomtom_data, air_data):
    """ inserts each doc to its collection, 
    assumes the connection to cluster and db is already active"""

    traffic_id = mydb.tomtom.insert_one(tomtom_data).inserted_id
    air_id = mydb.air.insert_one(air_data).inserted_id

    time = datetime.datetime.now().isoformat()

    print(f"You just inserted: {traffic_id} & {air_id}\nTime: {time}")

    return traffic_id, air_id, time

# REQUEST PARAMETERS (just for now here)------------------
in_lat = 46.0546089
in_long = 11.1138261

tomtom_data = rt_tomtom_request(in_lat, in_long)
air_data = rt_air_request(in_lat, in_long)
insert_docs(tomtom_data, air_data)

