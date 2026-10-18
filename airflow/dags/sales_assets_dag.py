from datetime import datetime
 
from airflow.sdk import DAG, Asset, task
 
sales_asset = Asset(name="sales", uri="s3://incoming/sales")
 
 
with DAG(
    dag_id="sales_producer_dag",
    description="sales asset-ini publish edir",
    start_date=datetime(2026, 10, 1),
    schedule=None,      
    catchup=False,
    tags=["lesson", "asset", "producer"],
) as producer_dag:
 
    @task(outlets=[sales_asset])  
    def publish_sales():
        print("Sales data hazırlandı, 'sales' asset event-i yaradılır")
 
    publish_sales()
 
 
with DAG(
    dag_id="sales_consumer_dag",
    description="sales asset-i yenilənəndə avtomatik başlayır",
    start_date=datetime(2026, 10, 1),
    schedule=[sales_asset],   
    catchup=False,
    tags=["lesson", "asset", "consumer"],
) as consumer_dag:
 
    @task
    def consume_sales(triggering_asset_events=None):
        for asset, events in (triggering_asset_events or {}).items():
            print(f"Başladan asset: {asset} | event sayı: {len(events)}")
        print("Sales data emal olunur...")
 
    consume_sales()