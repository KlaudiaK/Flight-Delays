#!/bin/sh

source ./env.sh

java -cp /usr/lib/kafka/libs/*:FlightDelays-assembly-0.1.0-SNAPSHOT.jar \
put.poznan.pl.klaudiak.producers.FlightsProducer data/flights 10 "${KAFKA_FLIGHTS_TOPIC_NAME}" \
1 "${KAFKA_BOOTSTRAP_SERVER}"
