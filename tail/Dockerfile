FROM python:3-slim-buster
COPY ./tail/ ./app/tail 
COPY ./config/ ./app/config
WORKDIR /app

RUN pip3 install kafka-python \
  && pip3 install pyyaml \
  && pip3 install pymongo

CMD python3 -u tail/tail_manager.py