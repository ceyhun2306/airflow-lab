from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

Filepath = "/opt/airflow/files/customers.csv"
Tablename = "customers"
Columns = ["customer_id", "full_name", "city", "registration_date", "membership_level"]


def load_csv_to_postgres(**context):
    import csv

    hook = PostgresHook(postgres_conn_id="postgres_etl")

    with open(Filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  
        rows = []
        for row in reader:
            customer_id, full_name, city, registration_date, membership_level = row
            city = city.strip().title()
            rows.append((customer_id, full_name, city, registration_date, membership_level))

    if not rows:
        raise ValueError("CSV faylinda sətir tapilmadi")

    hook.insert_rows(table=Tablename, rows=rows, target_fields=Columns)
    print(f"{len(rows)} sətir {Tablename} cədvəlinə yazildio")



with DAG(
    dag_id="csv_to_postgres_dag",
    description="CSV faylini yoxlayib Postgres-ə yükləyir, 4 saatdan bir işləyir",
    start_date=datetime(2026, 1, 1),
    schedule="0 */4 * * *",
    catchup=False,
    tags=["lesson", "etl"],
) as dag:


    check_file = FileSensor(
        task_id="check_file_exists",
        filepath=Filepath,
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
        subject="Airflow DAG Hesabatı: csv_to_postgres_dag",
        html_content="""
        <div style="font-family: Arial, sans-serif; padding: 20px; border: 1px solid #e0e0e0; border-radius: 5px; background-color: #f9f9f9;">
            <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">Airflow ETL Hesabatı</h2>
            <p><strong>DAG Adı:</strong> <code>csv_to_postgres_dag</code></p>
            <p><strong>Status:</strong> <span style="color: #27ae60; font-weight: bold;">Uğurla tamamlandı / İcra olundu</span></p>
            <p>CSV faylıardakı məlumatlar PostgreSQL bazasına uğurla yükləndi.</p>
            <br>
            <hr style="border: none; border-top: 1px solid #e0e0e0;">
            <p style="font-size: 12px; color: #7f8c8d;">Bu bildiriş Apache Airflow tərəfindən avtomatik göndərilmişdir.</p>
        </div>
        """,
        trigger_rule="all_done",
    )

    check_file >> load_data >> send_email