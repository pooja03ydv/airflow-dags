from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    dag_id="start_kubernetes_pod",
    default_args=default_args,
    schedule_interval=None
)

def start_pod():
    start_pod_task = KubernetesPodOperator(
        task_id='start_pod',
        name='my-pod',
        image='poyadav3/mavenbuild:66',
        namespace='default',
        get_logs=True,
        dag=dag
     )


start_pod_task = PythonOperator(
    task_id='start_pod_task',
    python_callable=start_pod,
    dag=dag
)
