Solution
![typeform_databricks-Page-1 drawio](https://user-images.githubusercontent.com/5692718/202158443-a832e06f-9fcb-43c8-b7fb-bc592011c33a.png)

# Running Locally

## Overview
This project has the following services running:
 - Kafka (https://github.com/obsidiandynamics/kafdrop)
 - A Kafka producer script (that generates events in Kafka)
 - Airflow (for orchestration)
 - Superset (for visualization)
 - A FastAPI service that mimics the Form events API endpoint

## Setting up

To get started, please make sure you have Docker installed on your system. You can download it here [here](https://docs.docker.com/get-docker/)

### Build Docker Image

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
