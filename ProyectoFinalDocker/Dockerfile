# Usar una imagen oficial de Python como imagen base
FROM python:3.12

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos de requerimientos a la imagen
COPY requirements.txt requirements.txt

# Instalar las dependencias de Python necesarias (incluyendo Airflow)
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip freeze > installed_packages.txt \
    && cat installed_packages.txt

# Crear directorio para DAGs
RUN mkdir -p /app/dags

# Copiar el archivo del DAG a la carpeta de dags de Airflow
COPY dags/my_dag.py /app/dags/my_dag.py

# Copiar toda la carpeta de scripts al contenedor
COPY scripts /app/scripts

# Inicializar la base de datos de Airflow y ejecutar el scheduler
CMD airflow db init && airflow scheduler
