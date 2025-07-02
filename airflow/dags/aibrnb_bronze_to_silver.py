from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.insert(0, '/opt/airflow')


from etl.airbnb.bronze.airbnb_extraction import extract_airbnb_data

from etl.airbnb.silver.transform_calendar import transform_calendar_silver
from etl.airbnb.silver.transform_listings import transform_listings_silver
from etl.airbnb.silver.transform_reviews import transform_reviews_silver

from etl.airbnb.gold.process_listings import process_listings_gold

from etl.airbnb.gold import process_listings


with DAG(
    dag_id="airbnb_etl_dag",
    start_date=datetime(2025, 4, 8),
    schedule_interval="@daily",
    catchup=False,
    tags=["airbnb", "etl"]) as dag:

    
    transform_reviews_task = PythonOperator(
        task_id="transform_reviews_data",
        python_callable=transform_reviews_silver
    )
    
    transform_listings_task = PythonOperator(
        task_id="transform_listings_data",
        python_callable=transform_listings_silver
    )
    
    transform_calendar_task = PythonOperator(
        task_id="transform_calendar_data",
        python_callable=transform_calendar_silver
    )
    
transform_reviews_task >> transform_listings_task >> transform_calendar_task