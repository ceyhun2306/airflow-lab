from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models.param import Param


@dag(
    dag_id="taskflow_demo_dag",
    description="TaskFlow API nümunəsi: @dag/@task, Param, Variable, Jinja templating",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"country": Param("AZ", enum=["AZ", "TR", "GE"])},
    tags=["lesson", "taskflow"],
)
def taskflow_demo_dag():

    @task
    def extract(ds=None, params=None):
        country = params["country"]
        sales = 1000
        return {"ds": ds, "country": country, "sales": sales}

    @task
    def transform(data: dict):
        data["sales"] = data["sales"] * 1.1
        return data

    @task
    def load(data: dict):
        print(f"Yekun nəticə: {data}")
        return data

    
    show_dates = BashOperator(
        task_id="show_dates",
        bash_command=(
            'echo "Bu gün: {{ ds }}" && '
            'echo "Dünən: {{ macros.ds_add(ds, -1) }}" && '
            'echo "Ölkə: {{ params.country }}" && '
            'echo "Şirkət: {{ var.value.company_name }}"'
        ),
    )
    

    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)
    loaded >> show_dates


taskflow_demo_dag()