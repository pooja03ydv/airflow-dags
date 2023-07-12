from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from kubernetes import client, config

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'create_kubernetes_pod_async',
    default_args=default_args,
    schedule_interval=None
)

def create_pod_async(payload):
    config.load_incluster_config()  # Load Kubernetes configuration for in-cluster access
    v1 = client.CoreV1Api()

    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "spring-app",
            "labels": {
                "app": "my-app"
            }
        },
        "spec": {
            "containers": [
                {
                    "name": "my-container",
                    "image": "poyadav3/mavenbuild:66",
                    "resources": {
                        "limits": {
                            "cpu": payload['cpu'],
                            "memory": payload['memory']
                        }
                    }
                }
            ]
        }
    }

    v1.create_namespaced_pod(body=pod_manifest, namespace="default")
    print("Pod created asynchronously.")

create_pod_task = KubernetesPodOperator(
    task_id='create_pod_async',
    op_kwargs={'payload': '{{ dag_run.conf["payload"] }}'},
    python_callable=create_pod_async,
    dag=dag
)
