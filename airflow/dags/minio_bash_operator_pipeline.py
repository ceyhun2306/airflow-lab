from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'matrix',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="minio_bash_operator_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["minio", "bash"]
) as dag:

    # 1. Qovluq yaradılması və bronze.py faylının hazırlanması
    setup_minio_and_dir = BashOperator(
        task_id="setup_minio_and_dir",
        bash_command="""
            mkdir -p /tmp/matrix && \
            echo "print('Bronze layer processing')" > /tmp/bronze.py && \
            echo "Setup and local file creation completed."
        """
    )

    # 2. bronze.py faylının MinIO-ya yüklənməsi
    upload_bronze_file = BashOperator(
        task_id="upload_bronze_file",
        bash_command="""
            python3 -c "
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='matrixuser',
    aws_secret_access_key='matrix1234'
)

buckets = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
if 'matrix' not in buckets:
    s3.create_bucket(Bucket='matrix')

s3.upload_file('/tmp/bronze.py', 'matrix', 'test_bash/bronze.py')
print('Successfully uploaded /tmp/bronze.py to MinIO -> matrix/test_bash/bronze.py')
"
        """
    )

    list_minio_files = BashOperator(
        task_id="list_minio_files",
        bash_command="""
            echo "==========================================" && \
            echo "MinIO Bucket 'matrix' / test_bash/ Folder Contents:" && \
            python3 -c "
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='matrixuser',
    aws_secret_access_key='matrix1234'
)

res = s3.list_objects_v2(Bucket='matrix', Prefix='test_bash/')
found = False
for obj in res.get('Contents', []):
    key_name = obj.get('Key')
    file_size = obj.get('Size')
    print(f'FILE IN MINIO: {key_name} | Size: {file_size} bytes')
    found = True

if not found:
    raise Exception('No files found in MinIO!')
" && \
            echo "=========================================="
        """
    )

    setup_minio_and_dir >> upload_bronze_file >> list_minio_files