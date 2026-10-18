import json
from datetime import datetime
 
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
 
AWS_CONN_ID = "minio_conn"   
BUCKET = "incoming"         
PREFIX = "raw/"              
 
 
with DAG(
    dag_id="minio_files_pipeline_dag",
    description="MinIO fayllarını siyahılayır, CSV/JSON-a görə budaqlandırır, dinamik emal edir",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["lesson", "minio", "mapping", "branching"],
) as dag:
 
    @task
    def list_files():
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        keys = hook.list_keys(bucket_name=BUCKET, prefix=PREFIX) or []
        keys = [k for k in keys if not k.endswith("/")]  
        print(f"Tapılan fayllar: {keys}")
        return keys
 
    @task
    def pick_csv(keys: list):
        return [k for k in keys if k.lower().endswith(".csv")]
 
    @task
    def pick_json(keys: list):
        return [k for k in keys if k.lower().endswith(".json")]
 
    @task.branch
    def choose_branch(keys: list):
        chosen = []
        if any(k.lower().endswith(".csv") for k in keys):
            chosen.append("process_csv")
        if any(k.lower().endswith(".json") for k in keys):
            chosen.append("process_json")
        print(f"Seçilən budaqlar: {chosen}")
        return chosen   
 
    @task
    def process_csv(key: str):
        text = S3Hook(aws_conn_id=AWS_CONN_ID).read_key(key=key, bucket_name=BUCKET)
        lines = text.strip().splitlines()
        result = {"file": key, "rows": max(len(lines) - 1, 0)}   
        print(f"CSV emal olundu: {result}")
        return result
 
    @task
    def process_json(key: str):
        text = S3Hook(aws_conn_id=AWS_CONN_ID).read_key(key=key, bucket_name=BUCKET)
        try:
            data = json.loads(text)
            count = len(data) if isinstance(data, list) else 1
        except json.JSONDecodeError:
            count = len([ln for ln in text.splitlines() if ln.strip()])
        result = {"file": key, "records": count}
        print(f"JSON emal olundu: {result}")
        return result
 
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )
 
    @task(trigger_rule="all_done")
    def cleanup():
        print("Cleanup: müvəqqəti resurslar təmizlənir (hər halda işləyir)")
 
    keys = list_files()
    csv_keys = pick_csv(keys)
    json_keys = pick_json(keys)
    branch = choose_branch(keys)
 
    csv_proc = process_csv.expand(key=csv_keys)     
    json_proc = process_json.expand(key=json_keys)  
 
    branch >> [csv_proc, json_proc]
    [csv_proc, json_proc] >> join >> cleanup()