"""Small DAG used by the airflow-cli worked examples.

Trigger it with a conf to change its behaviour:
    {"rows": 10}            how many rows `transform` processes
    {"fail": true}          make `transform` raise, to see failure handling
"""

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def transform(**context):
    params = context["params"]
    if params["fail"]:
        raise ValueError("asked to fail via conf")
    rows = int(params["rows"])
    return {"rows": rows, "sum_of_squares": sum(i * i for i in range(rows))}


with DAG(
    dag_id="cli_demo",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    params={"rows": 5, "fail": False},
    tags=["example", "airflow-cli"],
) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command="echo extracting {{ params.rows }} rows",
    )
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load = BashOperator(
        task_id="load",
        bash_command="echo loaded {{ ti.xcom_pull(task_ids='transform') }}",
    )
    extract >> transform_task >> load
