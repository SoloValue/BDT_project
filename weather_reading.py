import json

with open("weather.json") as weather:
    weather_data=weather.read()

weather_json=json.loads(weather_data)

weather_forecasts=weather_json["localita"]["previsione_giorno"]

days = []
for el in weather_forecasts:
    hourly_forecast=el['previsione_oraria']
    day = []
    for el1 in hourly_forecast:
        day.append(dict([("data", el['data']) ,("hour", el1['ora']) , ("precipitazioni",el1['precipitazioni']),("prob_prec",el1['probabilita_prec']),("wind",el1['vento']['intensita'])]))
    days.append(day)

print(days)
