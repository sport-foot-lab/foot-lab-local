# Image officielle Airflow + Cosmos + dbt (méthode standard « étendre l'image »)
FROM apache/airflow:3.4.0

# Cosmos dans Airflow
RUN pip install --no-cache-dir "apache-airflow==3.4.0" "astronomer-cosmos==1.16.0"

# dbt dans un dossier séparé (évite les conflits de versions avec Airflow)
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core==1.12.6" "dbt-postgres==1.11.0"

    
# Environnement Python des loaders dlthub (mêmes versions que le PC, séparé d'Airflow pour éviter les conflits)
RUN python -m venv /opt/airflow/ingestion_venv && \
    /opt/airflow/ingestion_venv/bin/pip install --no-cache-dir \
      "dlt[duckdb,postgres,parquet]==1.32.0" "duckdb==1.5.5" "python-dotenv==1.2.4"


# dbt dans un dossier séparé (évite les conflits de versions avec Airflow)
# Les DEUX adaptateurs, mêmes versions que le PC (requirements.txt) :
#   dbt-duckdb  -> target dev  (data/dev.duckdb)
#   dbt-postgres -> target prod (base datalab)
# duckdb 1.5.5 : même version que dlthub et duckdb.exe (le fichier dev.duckdb doit rester lisible par tous)
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core==1.12.6" "dbt-postgres==1.11.0" "dbt-duckdb==1.11.1" "duckdb==1.5.5"