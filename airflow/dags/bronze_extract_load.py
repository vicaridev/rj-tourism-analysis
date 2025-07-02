from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow')

from etl.airbnb.bronze.airbnb_extraction import extract_airbnb_data
from etl.airbnb.bronze.load_airbnb_bronze import load_airbnb_bronze




with DAG(dag_id="airbnb_bronze_el_task",
        start_date=datetime(2025, 4, 8),
        schedule_interval="@daily",
        catchup=False,
        tags=["extract", "etl"]) as dag:
    
        extract_airbnb_task = PythonOperator(
        task_id="extract_airbnb_data",
        python_callable=extract_airbnb_data
        )
        
        load_airbnb_bronze_task = PythonOperator(
        task_id='load_airbnb_bronze',
        python_callable=load_airbnb_bronze,
        execution_timeout=timedelta(minutes=10)
        )
        
extract_airbnb_task >> load_airbnb_bronze_task