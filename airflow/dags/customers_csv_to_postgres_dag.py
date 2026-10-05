from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

FILE_PATH = "/opt/airflow/files/customers.csv"
TABLE_NAME = "customers"
COLUMNS = ["customer_id", "full_name", "city", "registration_date", "membership_level"]


def load_csv_to_postgres(**context):
    import csv

    hook = PostgresHook(postgres_conn_id="postgres_etl")

    with open(FILE_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        rows = []
        for row in reader:
            customer_id, full_name, city, registration_date, membership_level = row
            city = city.strip().title()
            rows.append((customer_id, full_name, city, registration_date, membership_level))

    if not rows:
        raise ValueError("CSV faylında sətir tapılmadı")

    hook.insert_rows(table=TABLE_NAME, rows=rows, target_fields=COLUMNS)
    print(f"{len(rows)} sətir {TABLE_NAME} cədvəlinə yazıldı")


default_args = {
    "owner": "ceyhun",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customers_csv_to_postgres_dag",
    description="customers.csv faylını yoxlayıb Postgres-ə yükləyir, hər gün 4 saatdan bir işləyir",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 */4 * * *",
    catchup=False,
    tags=["lesson", "etl", "customers"],
) as dag:

    check_file = FileSensor(
        task_id="check_file_exists",
        filepath=FILE_PATH,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
    )

    load_data = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
    )

    send_email = EmailOperator(
        task_id="send_completion_email",
        to="ceyhunhemidov274@gmail.com",
        subject="Airflow DAG bitdi: customers_csv_to_postgres_dag",
        html_content="<p>customers_csv_to_postgres_dag icra olundu. Statusu Airflow UI-dən yoxla.</p>",
        trigger_rule="all_done",
    )

    check_file >> load_data >> send_email