## BDT_project

Repository for the Big Data Technologies project of group 7.<br>
Air Quality prediction using weather and traffic data.

---

***Abstract***

The project aims to predict the Air Quality of a city for the next 96 hours. It uses real time traffic data and weather forecast to compute the Air Quality Index for each hour for the next 4 days (96 values in total). Our structure consists of 4 components: a User Interface (UI), a manager for the system (Head), a connection between the APIs and our database (API_sink), and an Apache Spark node to compute our data into predictions (Tail). Each component will comunicate using Kafka message queue.

---

### Technologies

+ Apache Spark
+ Kafka + Zookeeper (with a multi-broker setup)
+ MongoDB
+ Docker + Docker-compose
+ Flask (Python framework)

---

### The Pipeline

![air-quality pipeline](https://github.com/SoloValue/BDT_project/assets/119301751/fe18eb64-0801-437d-9274-765754dc8f55)

---

### Project Files Structure

+ `head`, `api_sink`, `tail`: folders containing all the files used by the relative component. Inside each one we can find:
  + `Dockerfile`: file used to create the Docker image of the componet.
  + `<component>_manager.py`: file to run to start up the component.
+ `UI`: folder containing all the files required to the component. `app.py` is the file to run to start up the component.
+ `config`: folder of all the configuration files.
+ `example_json`, `kafka_setup`: files used for testing (can be ignored).
+ `docker-compose-services.yml`: file to run using docker-compose to set up all the services we use in our project:
  + Kafka + Zookeeper (3 brokers)
  + MongoDB + Mongo-express
  + Apache Spark
+ `docker-compose-managers.yml`: file to run using Docker-compose to start all the managers (__head__, __api_sink__, and __tail__).

---

### Kafka Network

![How does it work (3)](https://github.com/SoloValue/BDT_project/assets/119301751/6b31c4d9-d13d-4d33-aae0-6cab4e33041a)

---

### How to run

The project comes with 2 docker-compose files that manage everything but the __UI__. To do so, you first need to check the file `config/config.yaml` for two values (`"environment"`) and make sure they are set to _docker_ when creating the images for Docker (they should be already on _docker_, but if we change them by mistake than the managers will not work).

![config file](https://github.com/SoloValue/BDT_project/assets/119301751/bfd93a71-8ed2-4679-9888-64d27f0dfa09)

Then you can run the docker files:
```bash
docker-compose -f docker-compose-services.yml up
```

```bash
docker-compose -f docker-compose-managers.yml up
```

You shold now see the following messages on the bash running the `docker-compose-managers.yml` file:

![managers working](https://github.com/SoloValue/BDT_project/assets/119301751/09c96a18-a22b-4a51-bdf2-de5c95aeecb8)


And finally you can start the __UI__ by running:
```bash
python3 UI/app.py
```

Connecting to the address shown on the console (default is localhost:5000) you will be able to access the user interface and interact with our project.

---

### Dependencies

For this project we worked using:
+ Docker v24.0.2
+ Docker Compose v2.18.1
+ Python v3.11.0
+ Flask v2.3.2

---

### Authors

- Elisa Basso [@elisabasso00](https://github.com/elisabasso00)
- Matteo Moscatelli [@SoloValue](https://github.com/SoloValue)
- Sara Tegoni [@sraatgn](https://github.com/sraatgn)
