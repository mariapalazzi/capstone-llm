import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2026, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

AWS_ENV = {
    "AWS_S3_PATH": os.environ["AWS_S3_PATH"],
    "S3_USER": os.environ["S3_USER"],
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    "AWS_DEFAULT_REGION": os.environ["AWS_DEFAULT_REGION"],
}

params={
    "tag": Param(
        "python-polars",
        type="string",
        description="Stack Overflow tag to process",
    )
}

with DAG(
    dag_id="capstone_llm",
    default_args=default_args,
    params=params,
    schedule=None,
    catchup=False,
) as dag:

    ingest = DockerOperator(
        task_id="ingest",
        image="capstone-llm",
        command=[
            "uv", "run", "python",
            "-m", "capstonellm.tasks.ingest",
            "--tag", "{{ params.tag }}",
        ],
        environment=AWS_ENV,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
    )

    clean = DockerOperator(
        task_id="clean",
        image="capstone-llm",
        command=[
            "uv", "run", "python",
            "-m", "capstonellm.tasks.clean",
            "--tag", "{{ params.tag }}",
        ],
        environment=AWS_ENV,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
    )

    ingest >> clean