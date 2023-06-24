import pymongo
import yaml
from datetime import datetime as dt
from datetime import timedelta
import copy
import json

giorni={"feriale":["Monday", "Tuesday", "Wednesday", "Thuesday", "Friday", "Saturday"],
        "festivo": ["Sunday"]}

def pre_proc(db_api, db_PreProc, request_time):

    # weather ---------------------------------------
    weather_collection = db_api["weather"]
    weather_json = weather_collection.find_one({
        "request_time": request_time
        })
    previsione_giorno=weather_json["request_data"]["localita"]["previsione_giorno"]

    pp_weather = {"request_time": request_time}
    days_list = []
    for i,giorno in enumerate(previsione_giorno):
        previsione_oraria=giorno['previsione_oraria']
        
        for ora in previsione_oraria:
            days_list.append(dict([("request_time", request_time),
                            ("datetime", f"{giorno['data']}:{ora['ora']}"),                                   
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
    traffic_level=(free_flow_speed-current_speed)/free_flow_speed
    datatime1=traffic_json["request_time"]
    parsed_date=dt.strptime(datatime1, '%Y-%m-%dT%H:%M:%S.%f')
    datetime=parsed_date.strftime('%Y-%m-%d:%H')

    actual_traf=dict({"request_time":request_time})

    tomtom_traffic=[]
    for hour in range(97):
        date = parsed_date + timedelta(hours=hour)
        actual_traffic=copy.deepcopy(actual_traf)
        date=date.strftime('%Y-%m-%d:%H') 
        actual_traffic['datetime'] = date 
        day=dt.strptime(date, '%Y-%m-%d:%H')
        day_of_week = day.strftime('%A')
        actual_traffic["day_of_the_weeek"]=day_of_week
        tomtom_traffic.append(actual_traffic)

    with open( "./config/historical_tomtom.json", "r") as f:
        historical_tomtom=json.load(f)
        
        for i, row in enumerate(tomtom_traffic):
            if i==0:
                row["actual_traffic"]=traffic_level
            else:
                if row["day_of_the_weeek"] in giorni["feriale"]:
                    l=len(row['datetime'])
                    hour=row["datetime"][l-2:]
                    act_traf= historical_tomtom["feriale"][hour]
                    row["actual_traffic"] = act_traf
                else:
                    l=len(row['datetime'])
                    hour=row["datetime"][l-2:]
                    act_traf= historical_tomtom["festivo"][hour]
                    row["actual_traffic"] = act_traf
    
    #print(tomtom_traffic)
                
            


    # tomtom_collection= db_PreProc["tomtom"]
    # tomtom_collection.insert_one(tomtom_traffic)
    

    # #air quality index ---------------------------------
    aqi_collection = db_api["air"]
    air_json = aqi_collection.find_one({
          "request_time": request_time
          })
    
    i=air_json['request_data']
    air_forecasts=[]
    aqi_forecasts={"request_time":request_time}
    for el in i:
        aqi=el.get('aqi')
        datetime=el.get("datetime")
        air_forecasts.append(dict({"request_time":request_time, "datetime": datetime,"aqi": aqi}))

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
    request_time="2023-06-24T10:12:54.608381"
    pre_proc(db_api, db_pp, request_time)