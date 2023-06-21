from flask import Flask, render_template, url_for, request, redirect
from markupsafe import escape
from kafka import KafkaConsumer, KafkaProducer, errors
from serializers import serializer, deserializer
import yaml

## load config
CONFIG_PATH = "./config/config.yaml"
with open(CONFIG_PATH, "r") as f:
  config = yaml.safe_load(f)
  print(f"\tConfiguration file loaded from: {CONFIG_PATH}")
  PROJECT_ENV = config["project"]["environment"]

BROKER_ADD_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["address"] for i in range(3)]
BROKER_PORT_LIST = [config["kafka"][PROJECT_ENV][f"broker-{i+1}"]["port"] for i in range(3)]
TOPIC_PRODUCER = config["kafka"]["topics"]["input-head"]
TOPIC_CONSUMER = config["kafka"]["topics"]["input-head"]

app = Flask(__name__)

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/loading')
def loading():
  return render_template('loading.html')

@app.route('/data/<int:id>', methods=['GET', 'POST'])
def data(id):
  redirect('/loading')
  id_location = (id+1)*3-3
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
    return render_template('data.html', id_location=id_location)
  else:
    redirect('/')

if __name__ == "__main__":
  app.run(debug=True)