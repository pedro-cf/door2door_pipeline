#!/bin/bash
docker-compose down -t 0 -v || true ;
mkdir -p ./dags ./logs ./plugins ./resources
AIRFLOW_UID=$(id -u)
if ! grep -qxF "AIRFLOW_UID=$AIRFLOW_UID" .env ; then
    echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
fi
docker-compose up airflow-init &&
docker-compose up --build