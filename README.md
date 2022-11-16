Solution
![typeform_databricks-Page-1 drawio](https://user-images.githubusercontent.com/5692718/202158443-a832e06f-9fcb-43c8-b7fb-bc592011c33a.png)

# Running Locally

## Overview
This project has the following Dockerized services running:
 - Kafka (https://github.com/obsidiandynamics/kafdrop)
 - A Kafka producer script (that generates events in Kafka)
 - Airflow (for orchestration)
 - Superset (for visualization)
 - A FastAPI service that mimics the Form events API endpoint

 Why Kafka?

 Apart from this being a bonus task in the document, since forms can have different changes, it is best to go with the option of imagining these changes as events.

 Why Superset chosen for visualizing?

 It was chosen because it is open source and free, and can allow Data Analysts and Scientists, as well as BI developers query data, and build dashboards easily.

 Why Airflow and not Cron jobs?

 Although cron jobs can be used for scheduling the daily loads, it can get messy when there are a lot of other cron jobs, monitoring and scheduling, and re-running failed jobs becomes an issue with Cron jobs. This will not scale very well especially in production. With Airflow, we can scale different loads, and we can also monitor our ingestion.

## Setting up (Part 1)

To get started, please make sure you have Docker installed on your system. You can download it here [here](https://docs.docker.com/get-docker/)


Before building the docker image, it is important to explain how the flow works. The `development.env` file contains information related to the Kafka broker. This setup can work for both a local setup and kafka running on confluent. By default, the Kafka-broker is set to local

### Important notes

#### Local Setup
For a local setup please change the `SERVICE` to local and modify the `config.ini` file in the root directory to the details of your local kafka setup. (By default this would not need to be changed, as this project is configured to use the local setup)

```
SERVICE=confluent
CONFIG_FILENAME=confluent_config.ini
```


#### Confluent Kafka Setup
For a local setup please change the `SERVICE` to `confluent` and change the `CONFIG_FILENAME` to `confluent_config.ini` file in the root directory. Pleas make sure to edit the `confluent_config.ini` file to meet the setup for your Kafka confluent instance.

```
SERVICE=local
CONFIG_FILENAME=config.ini
```

The `NUM_OF_EVENTS` will throttle to the number of the producer to a particular number. If this number is less than 10, 10 would be used as the default.

### Building the Docker Image

Run the following in a terminal to build the Docker image, and run the service as the Docker container. Please make sure you are in the `typeform` directory (The first line does this for you)

```sh
cd typeform
bash start.sh
```
After waiting a few minutes all services should be running. The services can be accessed here:

 - Airflow can be accessed in http://localhost:8080
 - Kafdrop can be accessed in http://localhost:9000
 - Superset can be accessed in http://localhost:8088
 - FastAPI service can be accessed in http://localhost:8009/

## Connect to Database
You can connect to the Database using Superset by logging into Superset using these credentials:
- username: admin
- pwd: admin

To connect to the postgres DB, please use these  connection details:
- username: postgres
- pwd: postgres
- table_name: event_forms
- schema_name: public
- host-address: internal.docker.host:5432

If you prefer using a connection_id then use this `postgres://postgres:postgres@host.docker.internal:5432`

## Airflow
Since this is a daily job, the Airflow service has one Dag called 'daily_events_dag' and it is scheduled to run daily at 6:30 am. It consumes events from the Kafka broker, and updates the table, event_forms in Postgres.

## Queries
The answers for the questions can be found in a SQL file in the queries folder under the root directory.
