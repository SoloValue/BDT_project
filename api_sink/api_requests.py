#FUNCTIONS TO GET DATA FROM APIS
# LIBRARIES
import requests
import json
import pymongo 
import datetime
import yaml

# REAL TIME REQUESTS -------------------------------------
def rt_tomtom_request(lat, long_):

    """ retrieves current traffic-flow """

    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json?point={lat}%2C{long_}&unit=KMPH&openLr=false&key=FShxCHa2f5LlOL479fZRSYJJuTGBe3J4"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data_json = response.json()
    time = datetime.datetime.now().isoformat()

    result = {
        'request_time': time,
        'request_data': data_json['flowSegmentData']
    }
    
    return result 


def rt_air_request(lat, long_):

    """ retrieves hourly AQI for current hour & past 72hrs """

    url = f"https://api.weatherbit.io/v2.0/history/airquality?lat={lat}&lon={long_}&key=fd7a60de53d94247a0a16cfdec16d636"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data_json = response.json()
    time = datetime.datetime.now().isoformat()

    result = {
        'request_time': time,
        'request_data': data_json['data']
    }

    return result


def rt_weather_request(id_localita, days:int, language='en'):

    """ retrieves hourly weather forecast for next 96hrs """

    url = f"https://api.3bmeteo.com/publicv3/bollettino_meteo/previsioni_localita/{id_localita}/{days}/{language}/hourly/1?format=json2&X-API-KEY=xyCfMl5omzOMALMITS0oDkHJBptTXomH0tAbOleH"
    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data_json = response.json()
    time = datetime.datetime.now().isoformat()

    result = {
        'request_time': time,
        'request_data': data_json
    }
    
    return result


def get_all_requests(in_lat, in_long, id_localita, days, language='en'):
    """ performs all 3 requests at once, using the functions above """

    tomtom_data = rt_tomtom_request(in_lat, in_long)
    air_data = rt_air_request(in_lat, in_long)
    weather_data = rt_weather_request(id_localita, days, language)

    return tomtom_data, air_data, weather_data


# INSERT DATA FROM REQUESTS --------------------------------
def insert_docs(tomtom_data, air_data, weather_data):
    """ inserts each doc to its collection, 
    assumes the connection to cluster and db is already active"""

    traffic_id = mydb.tomtom.insert_one(tomtom_data).inserted_id
    air_id = mydb.air.insert_one(air_data).inserted_id
    weather_id = mydb.weather.insert_one(weather_data).inserted_id

    print(f"You just inserted: {traffic_id} & {air_id} & {weather_id}")

    return traffic_id, air_id, weather_id

# REQUEST PARAMETERS (just for now here)------------------
if __name__ == "__main__":

    # CONNECT TO MONGO ATLAS DATABASE 
    CONFIG_PATH = "./config/config.yaml"
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
    CONNECTION_STRING = config["mongodb"]["atlas"]["connection_string"]
    myclient = pymongo.MongoClient(CONNECTION_STRING)
    mydb = myclient["mydatabase"]

    in_lat = 46.0546089
    in_long = 11.1138261
    trento_id = 7428

    tomtom_data, air_data, weather_data = get_all_requests(in_lat, in_long, trento_id, 4)
    traffic_id, air_id, weather_id = insert_docs(tomtom_data, air_data, weather_data)
    

