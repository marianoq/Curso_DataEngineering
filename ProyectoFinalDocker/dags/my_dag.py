from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys

# Asegura que el script `main.py` está en el path
sys.path.append('/app/scripts')

def ejecutar_script():
    import main
    main.funcion_principal(

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mi_dag_diario',
    description='Un DAG que corre diariamente',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
)

tarea = PythonOperator(
    task_id='ejecutar_script',
    python_callable=ejecutar_script,
    dag=dag,
)
