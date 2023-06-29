"""
This is an example dag for using a Kubernetes Executor Configuration.
"""
from __future__ import annotations

import logging
import os

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff

log = logging.getLogger(__name__)

worker_container_repository = conf.get("kubernetes_executor", "worker_container_repository")
worker_container_tag = conf.get("kubernetes_executor", "worker_container_tag")

try:
    from kubernetes.client import models as k8s
except ImportError:
    log.warning(
        "The example_kubernetes_executor example DAG requires the kubernetes provider."
        " Please install it with: pip install apache-airflow[cncf.kubernetes]"
    )
    k8s = None


if k8s:
    with DAG(
        dag_id="example_kubernetes_executor",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example3"],
    ) as dag:
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
            print_stuff()

        four_task = task_with_resource_limits()
