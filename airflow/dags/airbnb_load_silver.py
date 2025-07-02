from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.insert(0, '/opt/airflow')

from etl.airbnb.silver.load_silver_data import load_airbnb_silver


with DAG(dag_id='airbnb_load_silver_task',
         start_date=datetime(2025, 4, 21),
         schedule_interval="@daily",
         catchup=False,
         tags=["load", "etl", "silver"]) as dag:
    
    load_airbnb_silver_task = PythonOperator(
        task_id="load_silver_data",
        python_callable=load_airbnb_silver
    )
    
load_airbnb_silver_task