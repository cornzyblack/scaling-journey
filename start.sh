#!/bin/bash

# Prepare  Superset service
git clone https://github.com/apache/superset.git superset
docker-compose -f $PWD/superset/docker-compose-non-dev.yml pull

# Prepare FastAPI service
cp $PWD/requirements.txt $PWD/main.py app

yes | cp -rf requirements.txt development utils.py app
docker-compose -f $PWD/app/docker-compose.yml pull

# Prepare Airflow service
cp $PWD/daily_events_dag.py $PWD/development $PWD/config.ini $PWD/airflow/dags

### Start all services

# Start Kafka broker
docker-compose -f $PWD/kafka-kafdrop/docker-compose.yml up -d

# Start FastAPI service
docker-compose -f $PWD/app/docker-compose.yml up -d

# Start Superset service
docker-compose -f $PWD/superset/docker-compose-non-dev.yml up -d

# Start Airflow service
docker-compose -f $PWD/airflow/docker-compose.yml up -d

# Start Kafka service
yes | cp -rf requirements.txt config.ini development utils.py producer.py kafka-service
docker-compose -f $PWD/kafka-service/docker-compose.yml up -d
