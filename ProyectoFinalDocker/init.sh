#!/bin/bash

# Inicializar la base de datos de Airflow
airflow db init

# Iniciar los servicios de Airflow
airflow webserver --port 8080 &
airflow scheduler
