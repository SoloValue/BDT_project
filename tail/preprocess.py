import pymongo
import yaml
from datetime import datetime as dt
from datetime import timedelta
import copy
import json
import os

def pre_proc(db_api, db_PreProc, request_time):
    # weather ---------------------------------------
    actual_hour= dt.strptime(request_time, '%Y-%m-%dT%H:%M:%S.%f')
    bottom_limit_str= actual_hour.strftime('%Y-%m-%d:%H')

    parsed_datetime = dt.strptime(bottom_limit_str, '%Y-%m-%d:%H')
    future_datetime = parsed_datetime + timedelta(hours=96)
    top_limit_str = future_datetime.strftime('%Y-%m-%d:%H')

    top_limit = dt.strptime(top_limit_str, '%Y-%m-%d:%H')
    bottom_limit = dt.strptime(bottom_limit_str, '%Y-%m-%d:%H')         

    weather_collection = db_api["weather"]
    weather_json = weather_collection.find_one({
        "request_time": request_time
        })
    previsione_giorno=weather_json["request_data"]["localita"]["previsione_giorno"]
    
    pp_weather = {}
    days_list = []
    for i,giorno in enumerate(previsione_giorno):
        previsione_oraria=giorno['previsione_oraria']
        
        for ora in previsione_oraria:
            days_list.append(dict([
                            ("datetime", (f"{giorno['data']}:{ora['ora']}")),      
                            ("precipitazioni",ora['precipitazioni']),
                            ("prob_prec",ora['probabilita_prec']),
                            ("wind",float(ora['vento']['intensita']))]))
    pp_weather["forecasts"] = days_list

    for item in pp_weather["forecasts"]:
        dt.strptime(item['datetime'], '%Y-%m-%d:%H')
   
    pp_process_weather={"request_time": request_time}
    forecasts=[]
    for el in pp_weather["forecasts"]:
        el['datetime']=dt.strptime(el['datetime'], '%Y-%m-%d:%H')
        if top_limit>=el["datetime"]>=bottom_limit:
            el['datetime']=el['datetime'].strftime('%Y-%m-%d:%H')
            forecasts.append(el)
    pp_process_weather['forecasts']=forecasts

    #print(pp_process_weather)

    wp_collection = db_PreProc["weather"]
    wp_collection.insert_one(pp_process_weather)


    ## traffic-----------------------------------------

    giorni={"feriale":["Monday", "Tuesday", "Wednesday", "Thuesday", "Friday", "Saturday"],
        "festivo": ["Sunday"]}

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

    actual_traf=dict()

    tomtom_traffic={"request_time":request_time}
    tomtom_traf=[]
    for hour in range(97):
        date = parsed_date + timedelta(hours=hour)
        actual_traffic=copy.deepcopy(actual_traf)
        date=date.strftime('%Y-%m-%d:%H') 
        actual_traffic['datetime'] = date 
        day=dt.strptime(date, '%Y-%m-%d:%H')
        day_of_week = day.strftime('%A')
        actual_traffic["day_of_the_weeek"]=day_of_week
        tomtom_traf.append(actual_traffic)

    traf_path = "./config/historical_tomtom.json"
    if not os.path.exists(traf_path):
        traf_path = "./app/config/historical_tomtom.json"
    with open(traf_path , "r") as f:
        historical_tomtom=json.load(f)
        
        for i, row in enumerate(tomtom_traf):
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
    tomtom_traffic["forecasts"]=tomtom_traf
    
    tomtom_collection= db_PreProc["tomtom"]
    tomtom_collection.insert_one(tomtom_traffic)
    

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
        air_forecasts.append(dict({"datetime": datetime,"aqi": aqi}))

    aqi_forecasts['forecasts']=air_forecasts

    air_collection = db_PreProc["air"]
    air_collection.insert_one(aqi_forecasts)

    return pp_process_weather, tomtom_traffic, aqi_forecasts




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