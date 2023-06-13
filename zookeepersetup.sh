#!/bin/sh
KAFKA_PATH="/mnt/c/kafka_2.13-3.4.0/"

${KAFKA_PATH}"bin/zookeeper-server-start.sh" ${KAFKA_PATH}"config/zookeeper.properties"
