from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import AsyncKubernetesPodOperator
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'kubernetes_pod_async',
    default_args=default_args,
    schedule_interval=None
)
async def create_pod_async():
    # Create and configure your Pod here
    # ...

create_pod_task = AsyncKubernetesPodOperator(
    task_id='create_pod_async',
    namespace='default',
    image='poyadav3/mavenbuild:66'
    dag=dag
)
