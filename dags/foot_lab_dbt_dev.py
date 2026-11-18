"""
=====================================================================
 foot_lab_dbt_dev.py — DAG Airflow : bronze -> dwh_* en DEV (DuckDB)
=====================================================================
 DÉCLENCHEMENT : automatique, quand foot_lab_landing_groupes a chargé le bronze DEV
                 (signal ASSET_BRONZE_DEV). Pas d'horaire propre.
 GRAPH         : lineage dbt rendu par Cosmos : source bronze -> modèle dwh_bronze -> ...
 FIN           : la tâche dev_ok émet ASSET_DBT_DEV -> signal attendu par foot_lab_dbt_prod.
 ⚠️ DuckDB UI FERMÉE pendant l'exécution (un seul écrivain DuckDB).
=====================================================================
"""

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from foot_lab_commun import ASSET_BRONZE_DEV, ASSET_DBT_DEV, groupe_dbt

with DAG(
    dag_id="foot_lab_dbt_dev",
    description="dbt DEV (DuckDB) : bronze -> dwh_bronze -> ... , lineage Cosmos",
    schedule=ASSET_BRONZE_DEV,           # démarre quand le bronze DEV vient d'être chargé
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # DuckDB : un seul écrivain à la fois
    is_paused_upon_creation=False,       # créé ACTIF : la dev suit automatiquement le bronze
    default_args={"retries": 0},
    tags=["foot-lab", "dbt", "dev"],
) as dag:

    dbt_dev = groupe_dbt("dev")          # toutes les tâches dbt, dans l'ordre du lineage

    # Tâche de fin : ne part que si TOUT dbt_dev est vert, puis émet le signal pour la prod
    dev_ok = BashOperator(
        task_id="dev_ok",
        bash_command="echo 'dbt DEV vert : signal envoyé à foot_lab_dbt_prod'",
        outlets=[ASSET_DBT_DEV],
    )

    dbt_dev >> dev_ok


    