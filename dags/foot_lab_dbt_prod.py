"""
=====================================================================
 foot_lab_dbt_prod.py — DAG Airflow : bronze -> dwh_* en PROD (Postgres)
=====================================================================
 DÉCLENCHEMENT : automatique, quand LES DEUX signaux sont reçus :
                   ASSET_DBT_DEV     (la DEV a réussi : contrôle avant prod)
                 & ASSET_BRONZE_PROD (le bronze PROD est chargé)
 CRÉÉ EN PAUSE : is_paused_upon_creation=True -> rien ne part en prod tant que
                 tu n'as pas activé le DAG (interrupteur dans l'interface).
 FIN           : prod_ok émet ASSET_DBT_PROD (pour la suite : gold, Power BI...).
=====================================================================
"""

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from foot_lab_commun import ASSET_BRONZE_PROD, ASSET_DBT_DEV, ASSET_DBT_PROD, groupe_dbt

with DAG(
    dag_id="foot_lab_dbt_prod",
    description="dbt PROD (Postgres) : après une DEV verte, lineage Cosmos",
    schedule=(ASSET_DBT_DEV & ASSET_BRONZE_PROD),   # « & » = attendre les 2 signaux
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,        # créé EN PAUSE : tu l'actives quand tu veux
    default_args={"retries": 0},
    tags=["foot-lab", "dbt", "prod"],
) as dag:

    dbt_prod = groupe_dbt("prod")

    prod_ok = BashOperator(
        task_id="prod_ok",
        bash_command="echo 'dbt PROD vert'",
        outlets=[ASSET_DBT_PROD],
    )

    dbt_prod >> prod_ok