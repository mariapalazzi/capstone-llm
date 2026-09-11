import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator

s3_user = os.environ["S3_USER"]

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

aws_env = {
    "AWS_S3_PATH": os.environ["AWS_S3_PATH"],
    "S3_USER": os.environ["S3_USER"],
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    "AWS_DEFAULT_REGION": os.environ["AWS_DEFAULT_REGION"],
}

params = {
    "tag": Param(
        "airflow",
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
        image="capstone-llm:latest",
        command=[
                "python3", "-m", "capstonellm.tasks.ingest",
                "-e", "docker",
                "-t", "{{ params.tag }}",
                "-u", s3_user,
            ],
            private_environment=aws_env,
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            api_version="auto",
            auto_remove="force",
            mount_tmp_dir=False,
    )

    clean = DockerOperator(
        task_id="clean",
        image="capstone-llm:latest",
        command=[
                "python3", "-m", "capstonellm.tasks.clean",
                "-e", "docker",
                "-t", "{{ params.tag }}",
                "-u", s3_user,
            ],
        private_environment=aws_env,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        api_version="auto",
        auto_remove="force",
        mount_tmp_dir=False,
    )

    ingest >> clean