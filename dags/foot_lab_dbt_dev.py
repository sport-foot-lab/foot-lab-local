"""
=====================================================================
 foot_lab_dbt_dev.py — DAG Airflow : bronze -> dwh_* en DEV (DuckDB)
=====================================================================
 DÉCLENCHEMENT : automatique, quand foot_lab_landing_groupes a chargé le bronze DEV
                 (signal ASSET_BRONZE_DEV). Pas d'horaire propre.
 ORDRE         : tests_unitaires -> dbt_dev (lineage Cosmos) -> dev_ok
 TESTS         : les tests unitaires dbt tournent AVANT tout le reste :
                 si un test échoue, aucune table n'est reconstruite.
 GRAPH         : lineage dbt rendu par Cosmos : source bronze -> snapshot -> modèle dwh_bronze -> ...
 FIN           : la tâche dev_ok émet ASSET_DBT_DEV -> signal attendu par foot_lab_dbt_prod.
 ⚠️ DuckDB UI FERMÉE pendant l'exécution (un seul écrivain DuckDB).
=====================================================================
"""

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from foot_lab_commun import ASSET_BRONZE_DEV, ASSET_DBT_DEV, DBT_EXE, DBT_PROJET, groupe_dbt

with DAG(
    dag_id="foot_lab_dbt_dev",
    description="dbt DEV (DuckDB) : tests unitaires puis bronze -> dwh_bronze -> ... , lineage Cosmos",
    schedule=ASSET_BRONZE_DEV,           # démarre quand le bronze DEV vient d'être chargé
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # DuckDB : un seul écrivain à la fois
    is_paused_upon_creation=False,       # créé ACTIF : la dev suit automatiquement le bronze
    default_args={"retries": 0},
    tags=["foot-lab", "dbt", "dev"],
) as dag:

    # ---------------------------------------------------------------------
    # Tâche 1 : les tests unitaires dbt (given/expect), AVANT le lineage
    #   --select "test_type:unit" : uniquement les tests unitaires
    #   --target dev              : DuckDB (comme le reste de ce DAG)
    #   --target-path / --log-path dans /tmp : dbt n'écrit rien dans ton dossier transformation\
    # ---------------------------------------------------------------------
    tests_unitaires = BashOperator(
        task_id="tests_unitaires",
        bash_command=(
            f"{DBT_EXE} test"
            f" --select 'test_type:unit'"
            f" --target dev"
            f" --project-dir {DBT_PROJET}"
            f" --profiles-dir {DBT_PROJET}"
            f" --target-path /tmp/dbt_tests_unitaires/target"
            f" --log-path /tmp/dbt_tests_unitaires/logs"
        ),
    )

    dbt_dev = groupe_dbt("dev")          # toutes les tâches dbt, dans l'ordre du lineage

    # Tâche de fin : ne part que si TOUT dbt_dev est vert, puis émet le signal pour la prod
    dev_ok = BashOperator(
        task_id="dev_ok",
        bash_command="echo 'dbt DEV vert : signal envoyé à foot_lab_dbt_prod'",
        outlets=[ASSET_DBT_DEV],
    )

    # tests unitaires verts -> lineage dbt -> signal pour la prod
    tests_unitaires >> dbt_dev >> dev_ok