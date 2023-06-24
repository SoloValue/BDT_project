from flask import Flask, render_template, request, redirect
from markupsafe import escape
from kafka import KafkaConsumer, KafkaProducer, errors
from serializers import serializer, deserializer
import yaml
from datetime import datetime
import csv

## load config
CONFIG_PATH = "./config/config.yaml"
with open(CONFIG_PATH, "r") as f:
  config = yaml.safe_load(f)
  #print(f"\tConfiguration file loaded from: {CONFIG_PATH}")
  PROJECT_ENV = config["project"]["environment"]

BROKER_ADD_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["address"] for i in range(3)]
BROKER_PORT_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["port"] for i in range(3)]
TOPIC_PRODUCER = config["kafka"]["topics"]["input-head"]
TOPIC_CONSUMER = config["kafka"]["topics"]["tail-output"]

app = Flask(__name__)

@app.route('/')
def index():
  buttons = ""
  with open('./config/cities.csv') as csv_f:
    csv_r = csv.reader(csv_f, delimiter=',')
    line_count = 0
    for row in csv_r:
      if line_count == 0:
        pass
      else:
        buttons += "<input "
        buttons += f'class="city-btn" type="submit" value="{row[0]}" formaction="/data/{row[2]}"'
        buttons += ">"
      line_count += 1
  return render_template('index.html', buttons = buttons)

@app.route('/data/<int:id_location>', methods=['GET', 'POST'])
def data(id_location):
  redirect('/loading')
  if request.method == 'POST':
    ## send input message
    producer = KafkaProducer(
      bootstrap_servers=[f'{BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}',
                         f'{BROKER_ADD_LIST[1]}:{BROKER_PORT_LIST[1]}',
                         f'{BROKER_ADD_LIST[2]}:{BROKER_PORT_LIST[2]}'],
      value_serializer = serializer
    )
    producer.send(TOPIC_PRODUCER, value={
      "status": "START",
      "id_location": id_location
    })

    consumer = KafkaConsumer(
      bootstrap_servers=[f'{BROKER_ADD_LIST[0]}:{BROKER_PORT_LIST[0]}',
                         f'{BROKER_ADD_LIST[1]}:{BROKER_PORT_LIST[1]}',
                         f'{BROKER_ADD_LIST[2]}:{BROKER_PORT_LIST[2]}'],
      value_deserializer = deserializer,
      auto_offset_reset='latest'
    )
    consumer.subscribe(topics=TOPIC_CONSUMER)
    #print(f'\tListening to topic: {TOPIC_CONSUMER}...')
    for message in consumer:
      if message.value["status"] != 'GREAT':
        pass
      request_time = datetime.fromisoformat(message.value["request_time"])
      predictions = message.value["predictions"]
      exp_traffic = message.value["exp_traffic"]
      predictions = message.value["predictions"]

      table = "<thead><th>Date</th><th>AQI</th><th>Expected Traffic</th></thead>"
      for i, pred in enumerate(predictions):
        if pred <= 50:
          color_pred = "rgb(151, 186, 251)" 
        elif pred > 100:
          color_pred = "rgb(255, 68, 68)"
        else:
          color_pred = "rgb(68, 200, 68)"

        table += "<tr>"
        table += f"<td>{(request_time.hour+i)%24}.00 {request_time.day+((request_time.hour+i)//24)}/{request_time.month}/{request_time.year}</td>"
        table += f"<td style='background-color: {color_pred} ;'>{pred}</td>"
        table += f"<td>{exp_traffic[i]}</td>"
        table += "</tr>"
        
      location = "Aula 16"
      with open('./config/cities.csv') as csv_f:
        csv_r = csv.reader(csv_f, delimiter=',')
        line_count = 0
        for row in csv_r:
          if line_count == 0:
            pass
          else:
            if row[2] == str(id_location):
              location = row[0]
          line_count += 1

      return render_template('data.html', table = table, location=location)
  else:
    redirect('/')

if __name__ == "__main__":
  app.run(debug=True)