### FUNCTIONS TO BE USED IN PROCESSING.py

def aqi_formula(traffic, computed, wind, betas={'traffic': 1.0, 'prec': -0.1, 'wind': -0.5}, hour=0, memo={}):
    """ 'hour' indicates the current hour we want to compute the aqi for (always start at 0)
        'memo' is for memoization purposes, from which the formula gets the x-1 aqi """

    if hour == 0:   # first hour
        previous_aqi = air_rdd.first()  # Assuming `rdd` is the RDD containing the AQI values
        memo[0] = previous_aqi
        hour += 1 # next hour
    elif hour < 96:
        previous_aqi = memo[hour - 1]      
        aqi = previous_aqi + traffic * betas['traffic'] + computed * betas['prec'] + wind * betas['wind']
        memo[hour] = aqi
        hour +=1
    else:
        print(f"Finished computing: {len(memo)} hrs.")

    return aqi

# for not not using this one below
def calculate_air_quality(combined_rdd, betas:dict):
    """ to be then mapped to the entire combined rdd """

    # Extract weather and traffic data from the combined RDD
    # You can access the weather and traffic data within the 'data' tuple using indexing.
    weather_data = combined_rdd[1][0] # use similar syntax according to our rdd
    traffic_data = combined_rdd[1][1]

    previous_aqi = 1
    traffic = 1
    computed = 1
    wind = 1
    betas = betas

    # Apply your air quality formula and return the result
    air_quality = aqi_formula(traffic, computed, wind, betas)
    return air_quality

### IN PROCESS.py
## Combine RDDs based on the hour key
# combined_rdd = weather_rdd.join(traffic_rdd)
## Apply the air quality formula to each record: The map transformation applies a given function to each 
## element of the RDD and returns a new RDD with the transformed results.
# output_rdd = combined_rdd.map(calculate_air_quality)  
# output_rdd = combined_rdd.map(lambda data_point: aqi_formula(data_point['hour'], data_point['traffic'], data_point['computed'], data_point['wind']))
   
