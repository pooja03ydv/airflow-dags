from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'create_pod_with_resource_limits',
     default_args=default_args,
    schedule_interval=None
)

def create_pod(payload):
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "my-pod",
            "labels": {
                "app": "my-app"
            }
        },
        "spec": {
            "containers": [
                {
                    "name": "spring",
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

    api_response = client.CoreV1Api().create_namespaced_pod(body=pod_manifest, namespace="default")
    print(api_response)

create_pod_task = PythonOperator(
    task_id='create_pod',
    python_callable=create_pod,
    op_kwargs={'payload': '{{ dag_run.conf }}'},
    dag=dag
)
