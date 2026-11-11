# Image officielle Airflow + Cosmos + dbt (méthode standard « étendre l'image »)
FROM apache/airflow:3.4.0

# Cosmos dans Airflow
RUN pip install --no-cache-dir "apache-airflow==3.4.0" "astronomer-cosmos==1.16.0"

# dbt dans un dossier séparé (évite les conflits de versions avec Airflow)
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core==1.12.6" "dbt-postgres==1.11.0"
