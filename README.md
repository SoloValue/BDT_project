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

![air-quality pipeline](https://github.com/SoloValue/BDT_project/assets/119301751/ae1d7b1f-ac50-4d2d-ad9c-33a98181d1b1)

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

![How does it work (3)](https://github.com/SoloValue/BDT_project/assets/119301751/bd4979bb-e019-44ac-a095-8cdcde078690)

---

### How to run

The project comes with 2 docker-compose files that manage everything but the __UI__. To do so, you first need to check the file `config/config.yaml` for two values (`"environment"`) and make sure they are set to _docker_ when creating the images for Docker (they should be already on _docker_, but if we change them by mistake than the managers will not work).

![config file](https://github.com/SoloValue/BDT_project/assets/119301751/944bde97-9683-44a0-b710-e43ef45719d4)

Then you can run the docker files:
```bash
docker-compose -f docker-compose-services.yml up
```

```bash
docker-compose -f docker-compose-managers.yml up
```

You shold now see the following messages on the bash running the `docker-compose-managers.yml` file:

![managers working](https://github.com/SoloValue/BDT_project/assets/119301751/c2883f7a-f6cd-4e82-a119-9f00ec23afcc)

And finally you can start the __UI__ by running:
```bash
python3 UI/app.py
```

Connecting to the address shown on the console (default is localhost:5000) you will be able to access the user interface and interact with our project.

Home page of the UI:
![UI_1](https://github.com/SoloValue/BDT_project/assets/119301751/cd6a2cf7-1903-4bec-9077-9718872c9d10)

UI with AQ predictions:
![UI_2](https://github.com/SoloValue/BDT_project/assets/119301751/2ccc0960-7dfb-47b0-9b4c-6f8a5e6faf22)

---

### To Be Implemented Yet

+ The formula we are currenty using is very simple and does not predict the air quality. We aim to generate a better model using data from our air API (previous 72 hours of AQI for that location), and past traffic data from the TomTom API, to better predict how the traffic condition will evolve since the request from the UI.
+ Switching the computation on Apache Spark to better manage high quantity of requests.
+ Add error control to our code and check messages to monitor the state of the pipeline, in order to atomize the procedure. Additionaly, if something should go wrong in the middle of the process, the pipeline will be able to recover from the last "checkpoint". 

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
