from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_heat():
    print("Heat DAG is alive! Ready to summarize Miami games 🏀")

with DAG(
    dag_id='heat_test',
    start_date=datetime(2026, 2, 1),
    schedule=None,  # manual trigger for now
    catchup=False,
    tags=['heat', 'test'],
) as dag:

    task = PythonOperator(
        task_id='heat_check',
        python_callable=say_heat,
    )