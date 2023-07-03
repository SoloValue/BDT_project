import requests
import pymongo
import time
from datetime import datetime
from crontab import CronTab

# CONNECTION TO MONGODB
connection_string = 'mongodb://localhost:27017'
myclient = pymongo.MongoClient(connection_string)
mydb = myclient['historical']

def fetch_and_insert(lat=46.065435, long_=11.113922):

    """ retrieves current traffic-flow, computes traffic_level and saves it in mongo """

    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json?point={lat}%2C{long_}&unit=KMPH&openLr=false&key=FShxCHa2f5LlOL479fZRSYJJuTGBe3J4"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    data_json = response.json()
    time = datetime.now().isoformat()

    traffic_level = (data_json['flowSegmentData']['freeFlowSpeed'] - data_json['flowSegmentData']['currentSpeed'])/data_json['flowSegmentData']['freeFlowSpeed']

    result = {
        'request_time': time,
        'traffic_level': traffic_level,
        'request_data': data_json['flowSegmentData']
    }

    traffic_id = mydb.tomtom.insert_one(result).inserted_id
    
    return traffic_id 

# Set the starting and ending date and time
start_date = datetime(2023, 6, 18, 0, 0)  # June 18, 2023, 00:00 AM
end_date = datetime(2023, 6, 20, 0, 0)    # June 20, 2023, 00:00 AM

# Wait until the starting date and time
while datetime.now() < start_date:
    time.sleep(60)  # Sleep for 1 minute before checking the starting time again

# Schedule API calls every hour until the ending date and time
while datetime.now() <= end_date:
    # Fetch data
    fetch_and_insert()
    
    # Wait for an hour
    time.sleep(3600)  

# Close the MongoDB connection
myclient.close()