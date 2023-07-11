from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'start_kubernetes_pod',
    default_args=default_args,
    schedule_interval=None
)

def start_pod(resource_limits):
    start_pod_task = KubernetesPodOperator(
        task_id='start_pod',
        name='my-pod',
        image='poyadav3/mavenbuild:66',
        namespace='default',
        get_logs=True,
        dag=dag
     )


