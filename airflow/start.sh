# Airflow
mkdir ./airflow/dags ./airflow/logs ./airflow/plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > ./airflow/env
docker-compose up airflow-init
docker-compose up -d
