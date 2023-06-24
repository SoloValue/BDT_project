import json 

with open("weather_process_data.json") as weather:
    weather_data=weather.read()

weather_json=json.loads(weather_data)

with open("air_quality.json") as air:
    air_data=air.read()

air_json=json.loads(air_data)


    