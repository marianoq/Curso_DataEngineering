from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import main

default_args = {
    'owner': 'MarianoQ',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Spotify_Data_Extraction',
    default_args=default_args,
    description='A DAG to extract top tracks from Spotify and insert them into a db Redshift',
    schedule_interval='@daily',
    start_date=datetime(2024, 6, 1),
    catchup=False
)

def extract_data():
    main()

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

extract_task
