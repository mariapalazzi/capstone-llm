import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from conveyor.operators import (
    ConveyorContainerOperatorV2,
    ConveyorSparkSubmitOperatorV2,
)


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

params = {
    "tag": Param(
        "airflow",
        type="string",
        description="Stack Overflow tag to process",
    )
}


with DAG(
    "capstone_llm-maria",
    default_args=default_args,
    params=params,
    schedule=None,
    catchup=False,
) as dag:
    
    ingest = ConveyorContainerOperatorV2(
        dag=dag,
        task_id="ingest",
        instance_type="mx.medium",
        aws_role="capstone_conveyor_llm",
        command=[
                "python3", "-m", "capstonellm.tasks.ingest"
            ],        
        arguments=["-t", "{{ params.tag }}"]
    )

    clean = ConveyorContainerOperatorV2(
        dag=dag,
        task_id="clean",
        instance_type="mx.medium",
        aws_role="capstone_conveyor_llm",
        command=[
                "python3", "-m", "capstonellm.tasks.clean",
            ],
        arguments=["-t", "{{ params.tag }}"],
    )

    # task dependancy
    ingest >> clean