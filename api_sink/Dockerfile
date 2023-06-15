FROM python:3-slim-buster
COPY ./api_sink/ ./app/api_sink 
COPY ./config/ ./app/config
WORKDIR /app

RUN pip3 install kafka-python \
  && pip3 install pymongo \
  && pip3 install pyyaml
CMD python3 -u api_sink/api_sink_manager.py
