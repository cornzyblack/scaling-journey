#!/usr/bin/env python
import json
import logging
import requests
from airflow import DAG
from pathlib import Path
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

bootstrap_server = "kafka:29092"
group_id = "python_kafka101_group_1"
topics = ["form.events"]
auto_offset_reset = "latest"
service = "local"

consumer_logger = logging.getLogger("airflow")

event_date = datetime.now().strftime("%Y%m%d")
daily_file_path = f"/opt/airflow/events/{event_date}/daily.json"


def consumer_function(message, prefix=None):
    if isinstance(message.key(), bytes):
        form_id = message.key().decode()
        value = json.loads(message.value().decode())

        req = requests.get(f"http://host.docker.internal:8009/form/{form_id}")
        req_data = req.json()
        value["payload"] = req_data

        try:
            data = json.load(open(daily_file_path, encoding="utf-8"))
        except Exception as e:
            consumer_logger.error(f"Error - > {e}")
            data = value
        if isinstance(data, dict):
            data = [data]
        data.append(value)
        consumer_logger.info(
            f"{prefix} {message.topic()} @ {message.offset()}; {form_id} : {value}")
        with open(daily_file_path, "w", encoding="utf8") as file_obj:
            json.dump(data, file_obj)
    return


with DAG(
    'daily_events_and_forms',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='A Daily run for Events',
    schedule_interval="30 6 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['daily_events_and_forms'],


) as dag:
    t0 = PostgresOperator(
        task_id="create_form_events_table",
        postgres_conn_id="postgres_master",
        sql="sql/form_events_schema.sql",
    )
    t1 = BashOperator(task_id="create_directory",
                      bash_command=f"mkdir -p /opt/airflow/events/{event_date}", dag=dag)

    t2 = ConsumeFromTopicOperator(
        task_id="consume_from_events_topic",
        topics=topics,
        apply_function="daily_events_dag.consumer_function",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": bootstrap_server,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": auto_offset_reset,
        },
        max_messages=10,
        max_batch_size=2,
        commit_cadence="end_of_batch",
    )
    sql_statement = "SELECT * FROM public.event_forms"
    if Path(daily_file_path).exists():
        with open(daily_file_path, "r", encoding="utf-8") as file_obj:
            current_events = json.load(file_obj)
            value_sql_statement = ""
            start_statement = "INSERT INTO public.event_forms (form_id, event_happened_at, event_type, payload) VALUES "
            value_array = []
            for event in current_events:
                form_id = event["form_id"]
                event_happened_at = datetime.strptime(
                    event["event_happened_at"], '%Y-%m-%d%H:%M:%S')
                event_happened_at = event_happened_at.strftime(
                    '%Y-%m-%d %H:%M:%S')
                event_type = event["event_type"]
                payload = json.dumps(event['payload'])
                value_array.append(
                    f"""('{form_id}', '{event_happened_at}', '{event_type}', '{payload}')""")
                value_sql_statement = "," .join(value_array)
            sql_statement = start_statement + value_sql_statement + ";"

    t4 = PostgresOperator(
        task_id="ingest_form_events_table",
        postgres_conn_id="postgres_master",
        sql=sql_statement,
    )

    t0 >> t1 >> t2 >> t4
