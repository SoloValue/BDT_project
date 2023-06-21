import pymongo
import yaml
from bson.objectid import ObjectId

# config ------------------------------------
CONFIG_PATH = "./config/config.yaml"
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# mongo client
MONGO_ENV = config["mongodb"]["environment"]
if MONGO_ENV == "atlas":
    CONNECTION_STRING = config["mongodb"][MONGO_ENV]["connection_string"]
    mongo_client = pymongo.MongoClient(CONNECTION_STRING)
else:
    MONGO_ADD = CONNECTION_STRING = config["mongodb"][MONGO_ENV]["address"]
    MONGO_PORT = CONNECTION_STRING = config["mongodb"][MONGO_ENV]["port"]
    username = config["mongodb"]["username"]
    username = config["mongodb"]["password"]
    mongo_client = pymongo.MongoClient(f'{MONGO_ADD}:{MONGO_PORT}',
                                username = "root",
                                password = "psw")
mongo_db = mongo_client["mydatabase"]
weather_collection = mongo_db["weather"]
'''with open("weather.json") as weather:
    weather_data=weather.read()

weather_json=json.loads(weather_data)
'''

weather_json = weather_collection.find_one({
    '_id': ObjectId('64915ca42ea98ef3fe875701')
    })
weather_forecasts=weather_json["localita"]["previsione_giorno"]

days = dict()
for i,el in enumerate(weather_forecasts):
    hourly_forecast=el['previsione_oraria']
    day = []
    for el1 in hourly_forecast:
        day.append(dict([("hour", el1['ora']) , ("precipitazioni",el1['precipitazioni']),("prob_prec",el1['probabilita_prec']),("wind",el1['vento']['intensita'])]))
    days[el['data']] = day

processed_db = mongo_client["processed_data"]
wp_collection = processed_db["weather"]
wp_collection.insert_one(days)
'''with open('weather_process_data.json', 'w') as file:
    json_data=json.dumps(days)
    file.write(json_data)
'''
