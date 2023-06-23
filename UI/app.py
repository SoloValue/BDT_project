from flask import Flask, render_template, url_for, request, redirect
from markupsafe import escape
from kafka import KafkaConsumer, KafkaProducer, errors
from serializers import serializer, deserializer
import yaml
from datetime import datetime

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
  return render_template('index.html')

@app.route('/loading')
def loading():
  return render_template('loading.html')

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
      now = datetime.now()
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

        table += f"<td>{now.hour+i}</td><td style='background-color: {color_pred} ;'>{pred}</td><td>0.1</td>"

        table += "</tr>"

      return render_template('data.html', table = table)
  else:
    redirect('/')

if __name__ == "__main__":
  app.run(debug=True)