FROM python:3-slim-buster
#FROM jupyter/pyspark-notebook:latest
COPY ./tail/ ./app/tail 
COPY ./config/ ./app/config

WORKDIR /app

RUN pip3 install kafka-python \
  && pip3 install pyyaml \
  && pip3 install pymongo
  
#CMD ls --recursive
#CMD ["spark-submit", "--master", "local[*]", "./app/tail/tail_manager.py"]
CMD python3 -u tail/tail_manager.py