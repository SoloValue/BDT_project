import pymongo
import yaml

def pre_proc(db_api, db_PreProc, request_time):

    # weather ---------------------------------------
    weather_collection = db_api["weather"]
    weather_json = weather_collection.find_one({
        "request_time": request_time
        })
    previsione_giorno=weather_json["request_data"]["localita"]["previsione_giorno"]

    pp_weather = dict()
    days_list = []
    for i,giorno in enumerate(previsione_giorno):
        previsione_oraria=giorno['previsione_oraria']
        
        for ora in previsione_oraria:
            days_list.append(dict([("datetime", f"{giorno['data']}:{ora['ora']}"),
                             ("precipitazioni",ora['precipitazioni']),
                             ("prob_prec",ora['probabilita_prec']),
                             ("wind",float(ora['vento']['intensita']))]))
    pp_weather["forecast"] = days_list

    wp_collection = db_PreProc["weather"]
    wp_collection.insert_one(pp_weather)
    

    # # traffic-----------------------------------------

    traffic_collection=db_api["tomtom"]
    traffic_json=traffic_collection.find_one({
        "request_time": request_time
        })
    
    current_speed=traffic_json['request_data']['currentSpeed']
    free_flow_speed=traffic_json['request_data']['freeFlowSpeed']
    actual_traffic=(free_flow_speed-current_speed)/free_flow_speed

    actual_traffic=dict({"datetime": f"{giorno['data']}:{ora['ora']}","actual_traffic": actual_traffic})

    tomtom_collection = db_PreProc["tomtom"]
    tomtom_collection.insert_one(actual_traffic)

    # #air quality index ---------------------------------
    aqi_collection = db_api["air"]
    air_json = aqi_collection.find_one({
          "request_time": request_time
          })
    
    i=air_json['request_data']
    air_forecasts=[]
    aqi_forecasts=dict()
    for el in i:
        aqi=el.get('aqi')
        datetime=el.get("datetime")
        air_forecasts.append(dict({"datetime": datetime,"aqi": aqi}))

    aqi_forecasts['forecasts']=air_forecasts

    air_collection = db_PreProc["air"]
    air_collection.insert_one(aqi_forecasts)
    


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