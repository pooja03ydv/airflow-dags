from __future__ import annotations
import logging
import os
import pendulum
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from kubernetes import client as k8s

with DAG(
    dag_id="hello_world",
    schedule_interval='@daily',
    start_date=datetime(2017, 3, 20), 
    catchup=False,
    tags=["example3"],
) as dag:

    # Use k8s_client.V1ResourceRequirements to define resource limits
    k8s_resource_requirements = k8s.V1ResourceRequirements(
        requests={"memory": "512Mi"}, limits={"memory": "512Mi"}
    )

    kube_exec_config_resource_limits = {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s_resource_requirements,
                    )
                ],
                affinity=k8s_affinity,
                tolerations=k8s_tolerations,
            )
        )
    }

    @task(executor_config=kube_exec_config_resource_limits)
    def task_with_resource_limits():
        return BashOperator(task_id='sleep', bash_command='sleep 300')

    four_task = task_with_resource_limits()
    four_task >> DummyOperator(task_id="dummy_task")
