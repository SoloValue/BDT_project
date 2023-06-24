FROM python:3-slim-buster
COPY ./head/ ./app/head 
COPY ./config/ ./app/config
WORKDIR /app

RUN pip3 install kafka-python \
  && pip3 install pyyaml \
  && pip3 install pymongo
#CMD ls --recursive /app
CMD python3 -u head/head_manager.py