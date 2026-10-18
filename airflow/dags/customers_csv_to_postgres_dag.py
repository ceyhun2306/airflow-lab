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
        task_id="customers_csv_to_postgres_dag",
        to="ceyhunhemidov274@gmail.com",
        subject="Airflow DAG bitdi: customers_csv_to_postgres_dag",
        html_content="""
        <div style="margin:0;padding:32px 16px;background-color:#f3f6fb;font-family:Arial,sans-serif;color:#1f2937;">
          <div style="max-width:560px;margin:0 auto;background-color:#ffffff;border:1px solid #e5eaf2;border-radius:12px;overflow:hidden;">
            <div style="padding:24px 28px;background-color:#173b66;color:#ffffff;">
              <p style="margin:0 0 8px;font-size:12px;letter-spacing:1.5px;text-transform:uppercase;color:#c7dcf5;">Airflow bildirişi</p>
              <h1 style="margin:0;font-size:22px;line-height:1.35;">DAG icrası başa çatdı</h1>
            </div>
            <div style="padding:28px;">
              <p style="margin:0 0 20px;font-size:15px;line-height:1.6;">
                <strong>customers_csv_to_postgres_dag</strong> DAG-ının icra cəhdi tamamlandı.
                Yekun nəticəni aşağıdakı tapşırığın statusundan yoxlaya bilərsiniz.
              </p>
              <div style="padding:16px 18px;background-color:#f3f6fb;border-radius:8px;">
                <p style="margin:0 0 6px;font-size:12px;color:#64748b;">Yoxlanılacaq tapşırıq</p>
                <p style="margin:0;font-size:14px;font-weight:bold;color:#173b66;">customers_csv_to_postgres_dag</p>
              </div>
              <p style="margin:22px 0 0;font-size:13px;line-height:1.6;color:#64748b;">
                DAG və tapşırıqların uğurlu və ya uğursuz icra vəziyyətini Airflow UI-də yoxlayın.
              </p>
            </div>
            <div style="padding:16px 28px;border-top:1px solid #e5eaf2;font-size:12px;color:#94a3b8;">
              Avtomatik bildiriş · Airflow
            </div>
          </div>
        </div>
        """,
        trigger_rule="all_done",
    )

    check_file >> load_data >> send_email