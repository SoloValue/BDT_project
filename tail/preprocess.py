import pymongo
import yaml
from bson.objectid import ObjectId

def pre_proc(db_api, db_PreProc, request_time):

    # weather ---------------------------------------
    weather_collection = db_api["weather"]
    weather_json = weather_collection.find_one({
        '_id': ObjectId('64915ca42ea98ef3fe875701')
        })
    weather_forecasts=weather_json["request_data"]["localita"]["previsione_giorno"]

    days = dict()
    for i,el in enumerate(weather_forecasts):
        hourly_forecast=el['previsione_oraria']
        day = []
        for el1 in hourly_forecast:
            day.append(dict([("hour", el1['ora']) , ("precipitazioni",el1['precipitazioni']),("prob_prec",el1['probabilita_prec']),("wind",el1['vento']['intensita'])]))
        days[el['data']] = day

    wp_collection = db_PreProc["weather"]
    wp_collection.insert_one(days)



if __name__ == "__main__":
    # config ------------------------------------
    CONFIG_PATH = "./config/config.yaml"
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)