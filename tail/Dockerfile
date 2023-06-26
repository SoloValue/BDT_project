FROM jupyter/pyspark-notebook:latest
COPY ./tail/ ./app/tail 
COPY ./config/ ./app/config

RUN pip3 install kafka-python \
  && pip3 install pyyaml \
  && pip3 install pymongo
#CMD ls --recursive
CMD ["spark-submit", "--master", "local[*]", "./app/tail/tail_manager.py"]