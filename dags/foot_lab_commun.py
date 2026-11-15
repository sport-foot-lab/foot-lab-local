"""
=====================================================================
 foot_lab_commun.py — réglages PARTAGÉS par les DAG foot-lab
=====================================================================
 Ce fichier ne contient AUCUN DAG : Airflow le lit, n'y trouve pas de DAG, et l'ignore.
 Les DAG l'importent (from foot_lab_commun import ...) : une seule définition
 des Assets et de la configuration dbt -> dev et prod ISO par construction.
=====================================================================
"""

from airflow.sdk import Asset
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import SourceRenderingBehavior

# ---------------------------------------------------------------------
# 1. ASSETS : les "signaux" échangés entre DAG
#    Un DAG qui a une tâche avec outlets=[ASSET] annonce "j'ai mis à jour ceci".
#    Un DAG avec schedule=ASSET démarre quand il reçoit ce signal.
# ---------------------------------------------------------------------
ASSET_BRONZE_DEV = Asset("foot_lab_bronze_dev")      # émis par foot_lab_landing_groupes (bronze.dev vert)
ASSET_BRONZE_PROD = Asset("foot_lab_bronze_prod")    # émis par foot_lab_landing_groupes (bronze.prod vert)
ASSET_DBT_DEV = Asset("foot_lab_dbt_dev")            # émis par foot_lab_dbt_dev (dbt dev vert)
ASSET_DBT_PROD = Asset("foot_lab_dbt_prod")          # émis par foot_lab_dbt_prod (dbt prod vert)


# ---------------------------------------------------------------------
# 2. dbt (Cosmos) : chemins VUS DEPUIS le conteneur
# ---------------------------------------------------------------------
DBT_PROJET = "/opt/airflow/transformation"           # = ton dossier transformation\ (monté par le compose)
DBT_EXE = "/opt/airflow/dbt_venv/bin/dbt"            # dbt de l'image (dbt-duckdb + dbt-postgres)


def nom_affiche(node):
    """Nom AFFICHÉ d'une case dbt dans le Graph = la TABLE concernée (schéma.table).

    Par défaut Cosmos affiche <modèle>_run / <source>_source (nom de l'ACTION).
    Ici on affiche la table : bronze.api_football_fixtures -> dwh_bronze.api_football_fixtures.
    Seul l'affichage change : l'identifiant technique (task_id, logs) reste celui de Cosmos.
    """
    type_noeud = node.resource_type.value          # "model", "source", "test", "snapshot"...
    if type_noeud == "source":
        return node.unique_id.split(".", 2)[2]     # source.foot_lab.bronze.xxx -> bronze.xxx
    if type_noeud in ("model", "snapshot"):
        return f"{node.config.get('schema')}.{node.name}"   # ex. dwh_bronze.api_football_fixtures
    return f"{type_noeud} {node.name}"             # autres (tests...) : type + nom


def groupe_dbt(target):
    """Crée le groupe de tâches Cosmos du projet dbt foot_lab pour un target (dev ou prod).

    MÊME projet, MÊME rendu : seul le target change -> dev et prod ISO.
    """
    return DbtTaskGroup(
        group_id=f"dbt_{target}",                                  # dbt_dev / dbt_prod
        project_config=ProjectConfig(DBT_PROJET),                  # où est le dbt_project.yml
        profile_config=ProfileConfig(
            profile_name="foot_lab",                               # = profile: du dbt_project.yml
            target_name=target,                                    # dev -> DuckDB ; prod -> Postgres
            profiles_yml_filepath=f"{DBT_PROJET}/profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXE),
        render_config=RenderConfig(
            # LINEAGE : chaque source bronze devient une case reliée aux modèles qui la lisent
            source_rendering_behavior=SourceRenderingBehavior.ALL,
            source_pruning=True,                                   # seulement les sources UTILISÉES
            normalize_task_display_name=nom_affiche,               # cases nommées schéma.table
        ),
        operator_args={"install_deps": False},                     # pas de packages dbt -> pas de "dbt deps"
    )