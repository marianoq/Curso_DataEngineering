from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/app/scripts')

from main import main  # Importar la función principal de main.py

# Función que será ejecutada por el PythonOperator
def my_task():
    main()

# Definir los argumentos por defecto para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'spotify_dag_diario',
    description='Un DAG que se ejecuta diariamente',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Definir la tarea utilizando PythonOperator
run_my_task = PythonOperator(
    task_id='download_spotify_data',
    python_callable=my_task,
    dag=dag
)

# Establecer el orden de ejecución de las tareas
run_my_task
