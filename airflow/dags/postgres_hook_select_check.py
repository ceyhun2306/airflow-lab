import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'matrix',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def run_postgres_select_one():
    # Şəkildəki Connection ID daxil edilir: postgres_etl
    pg_hook = PostgresHook(postgres_conn_id="postgres_etl")
    
    # Bazaya qoşulub SELECT 1 sorğusunu icra edirik
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    
    # Nəticəni Airflow task log-larına yazdırırıq
    logging.info("==========================================")
    logging.info("PostgresHook Query Executed Successfully!")
    logging.info(f"SELECT 1 Result: {result[0]}")
    logging.info("==========================================")
    
    cursor.close()
    conn.close()

with DAG(
    dag_id="postgres_hook_select_check",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["postgres", "hook"]
) as dag:

    test_postgres_connection = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=run_postgres_select_one
    )

    test_postgres_connection