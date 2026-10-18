from datetime import datetime
 
from airflow.sdk import DAG, task
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
 
 
with DAG(
    dag_id="source_dag",
    description="load_source task-ı olan mənbə DAG",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["lesson", "external-sensor", "source"],
) as source_dag:
 
    @task
    def load_source():
        print("Mənbə data yükləndi")
 
    load_source()
 
 
with DAG(
    dag_id="report_waiter_dag",
    description="source_dag.load_source bitənə qədər gözləyir, sonra hesabat hazırlayır",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["lesson", "external-sensor", "deferrable"],
) as waiter_dag:
 
    wait_for_source = ExternalTaskSensor(
        task_id="wait_for_load_source",
        external_dag_id="source_dag",
        external_task_id="load_source",
        deferrable=True,     
        poke_interval=30,    
        timeout=900,         
    )
 
    @task
    def build_report():
        print("source_dag.load_source bitdi, hesabat hazırlanır")
 
    wait_for_source >> build_report()