### FUNCTIONS TO BE USED IN PROCESSING.py

def aqi_formula(traffic, prec, prob_prec, wind, aqi0, betas={'traffic': 1.0, 'prec': -0.1, 'wind': -0.5}, hour=0, memo={}):
    """ 'hour' indicates the current hour we want to compute the aqi for (always start at 0)
        'memo' is for memoization purposes, from which the formula gets the x-1 aqi """

    if hour == 0:   # first hour
        previous_aqi = aqi0
        memo[0] = previous_aqi
        hour += 1 # next hour
    elif hour < 97:
        previous_aqi = memo[hour - 1]      
        aqi = previous_aqi + traffic * betas['traffic'] + (prec + prob_prec) * betas['prec'] + wind * betas['wind']
        memo[hour] = aqi
        hour +=1
    else:
        print(f"Finished computing: {len(memo)} hrs.")

    return aqi