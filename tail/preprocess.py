import pymongo
import yaml

def pre_proc(db_api, db_PreProc, request_time):

    # weather ---------------------------------------
    weather_collection = db_api["weather"]
    weather_json = weather_collection.find_one({
        "request_time": request_time
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

    # # traffic-----------------------------------------
    traffic_collection=db_api["tomtom"]
    traffic_json=traffic_collection.find_one({
        "resquest_time": request_time
        })
    
    #current_speed=traffic_json["request_data"]
    #free_flow_speed=traffic_json['request_data']
    #actual_traffic=(free_flow_speed-current_speed)/free_flow_speed

    # actual_traffic=dict({'currentSpeed': current_speed,'freeFlowSpeed': free_flow_speed,"actual_traffic": actual_traffic})

    # tomtom_collection = db_PreProc["tomtom"]
    # tomtom_collection.insert_one(actual_traffic)

    # #air quality index ---------------------------------
    # aqi_collection = db_api["air"]
    # air_json = aqi_collection.find_one({
    #     "resquest_time": request_time
    #     })
    # air_forecasts=air_json["request_data"]
    
    # air_days = dict()
    # for i,el in enumerate(air_forecasts):
    #     hourly_forecast=el['']
    #     day = []



    #     for el1 in hourly_forecast:
    #         day.append(dict([("hour", el1['ora']) , ("precipitazioni",el1['precipitazioni']),("prob_prec",el1['probabilita_prec']),("wind",el1['vento']['intensita'])]))
    #     days[el['data']] = day

    # wp_collection = db_PreProc["weather"]
    # wp_collection.insert_one(days)




if __name__ == "__main__":
    # config ------------------------------------
    CONFIG_PATH = "./config/config.yaml"
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

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
    db_api=mongo_client["mydatabase"]
    db_pp=mongo_client["preprocess_data"]
    request_time='2023-06-20T09:14:17.952133'
    pre_proc(db_api, db_pp, request_time)