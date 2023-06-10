FROM python:3-slim-buster
COPY . /app
WORKDIR /app

RUN pip3 install kafka-python
CMD python3 api_sink_manager.py