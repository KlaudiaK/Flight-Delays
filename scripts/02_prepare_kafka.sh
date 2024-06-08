#!/bin/sh

source ./env.sh

echo "Deleting existing kafka topics"
kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic flights-input || true
kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic flights-output || true
kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic anomalies-input || true
kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic anomalies-output || true

echo "Creating kafka topics"
kafka-topics.sh --create --topic flights-input --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --replication-factor 1 --partitions 1
kafka-topics.sh --create --topic flights-output --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --replication-factor 1 --partitions 1
kafka-topics.sh --create --topic anomalies-input --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --replication-factor 1 --partitions 1
kafka-topics.sh --create --topic anomalies-output --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --replication-factor 1 --partitions 1

echo "Done"
