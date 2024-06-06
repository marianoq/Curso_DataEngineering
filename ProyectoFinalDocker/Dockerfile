# Usa una imagen base de Python ligera
FROM python:3.12

# Instala las dependencias necesarias para Apache Airflow y el script
RUN pip install --no-cache-dir apache-airflow spotipy pandas tabulate psycopg2-binary

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos de tu proyecto
COPY dags/ ./dags/  
COPY requirements.txt . 

# Instala las dependencias del proyecto
RUN pip install --no-cache-dir -r requirements.txt

# Copia el archivo de DAG a la carpeta de Airflow
COPY dags/my_dag.py /usr/local/airflow/dags/my_dag.py

# Configura la variable de entorno de Airflow
ENV AIRFLOW_HOME=/usr/local/airflow

# Inicializa la base de datos de Airflow
RUN airflow db init

# Comando para ejecutar Airflow en el contenedor
CMD ["airflow", "scheduler"]
