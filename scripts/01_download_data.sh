#!/bin/sh

source ./env.sh

echo "Copying FlightDelays-assembly-0.1.0-SNAPSHOT.jar from bucket"
hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/FlightDelays-assembly-0.1.0-SNAPSHOT.jar
hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/spark_process.py

echo "Copying airports folder from bucket"
hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/airports

echo "Copying flights folder from bucket"
hadoop fs -copyToLocal gs://"${BUCKET_NAME}"/flights

echo "Preparing folders for data"
mkdir data
mv airports/ data/
mv flights/ data/

echo "Unzipiping flight files"
unzip data/flights/flights-2015.zip -d data/flights/

echo "Cleaning up"
rm -rf data/flights/flights-2015.zip

echo "Done! Data ready to be processed"
