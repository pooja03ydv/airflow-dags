from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

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
        resources=resource_limits,
        dag=dag,
        dag_run_timeout=datetime.timedelta(minutes=5)  # Set the timeout duration for the Pod
    )

start_pod_task = PythonOperator(
    task_id='start_pod_task',
    python_callable=start_pod,
    op_args=[{'cpu': '1', 'memory': '1Gi'}],
    dag=dag
)
