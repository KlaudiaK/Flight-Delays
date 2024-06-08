#!/bin/bash

echo "Start setting up cloud parameters"
export CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
export BUCKET_NAME="flight-data-spark-structured-streaming"
echo "Cloud parameters set up successfully"


echo "Start setting up kafka parameters"
export KAFKA_FLIGHTS_TOPIC_NAME="flights-input"
export KAFKA_BOOTSTRAP_SERVER="${CLUSTER_NAME}-w-0:9092"
echo "Kafka parameters set up successfully"

# JDBC parameters
echo "Setting up JDBC parameters..."
export JDBC_URL="jdbc:postgresql://localhost:8432/delays_db"
export JDBC_USERNAME="user"
export JDBC_PASSWORD="password"
export JDBC_DATABASE="delays_db"
export PGPASSWORD='mysecretpassword'
echo "JDBC parameters set up successfully."