"""
=====================================================================
 foot_lab_landing.py — DAG Airflow : API -> landing (API-Football + Open-Meteo)
=====================================================================
 RÔLE
   Lancer, dans l'ordre, les loaders qu'on lance aussi à la main :
     1. api_football_fixtures_l_loader.py            (les matchs)
     2a. api_football_top_l_loader.py --table …       (les 4 classements, l'un après l'autre)
     2b. open_meteo_geocodage_l_loader.py             (villes des stades -> coordonnées)
         puis open_meteo_meteo_journaliere_l_loader.py (météo des jours de match, 2 000 appels max)
   2a et 2b partent EN MÊME TEMPS après fixtures : ce sont 2 API différentes,
   chacune garde ses propres limites.

 PRINCIPE
   Chaque tâche = la MÊME commande que dans le terminal VS Code, mais lancée
   dans le conteneur Airflow avec l'environnement Python des loaders
   (/opt/airflow/ingestion_venv, mêmes versions que le PC).
   Script terminé proprement (exit 0, y compris « Rien à charger ») -> tâche VERTE.
   Erreur (quota, clé, API) -> tâche ROUGE, les suivantes de SA branche ne partent pas.

 PLANIFICATION
   schedule="0 */2 * * *" : toutes les 2 heures (test). Plus tard : "@daily" (1 fois par jour).
   Si tout est déjà complet en landing, chaque exécution s'arrête sur « Rien à charger » : 0 appel API.
   Météo : 2 000 appels max par exécution -> les gros rattrapages se font en plusieurs exécutions,
   espacées de 2 h (sous la limite Open-Meteo de 5 000 appels / heure).
=====================================================================
"""

from datetime import datetime

from airflow.sdk import DAG                                       # Airflow 3 : définition d'un DAG
from airflow.providers.standard.operators.bash import BashOperator  # tâche = une commande shell

# ---------------------------------------------------------------------
# PARTIE 1 — Chemins DANS le conteneur Airflow
# ---------------------------------------------------------------------
PYTHON = "/opt/airflow/ingestion_venv/bin/python"     # Python des loaders (dlthub 1.32, duckdb 1.5.5)
INGESTION = "/opt/airflow/ingestion"                  # = ton dossier ingestion\ (monté par le compose)

# Les 4 classements, lancés L'UN APRÈS L'AUTRE (jamais en même temps :
# l'API-Football limite le nombre d'appels par minute)
TABLES_TOP = ["topscorers", "topassists", "topyellowcards", "topredcards"]


# ---------------------------------------------------------------------
# PARTIE 2 — Le DAG
# ---------------------------------------------------------------------
with DAG(
    dag_id="foot_lab_landing",
    description="API -> landing : API-Football (fixtures, top*) + Open-Meteo (géocodage, météo)",
    schedule=None,            # toutes les 2 heures, à l'heure pile (cron, heure UTC)
    # schedule="@daily",               # plus tard : 1 fois par jour (à minuit UTC)
    start_date=datetime(2026, 11, 1),
    catchup=False,                     # ne pas rattraper les dates passées
    max_active_runs=1,                 # une seule exécution à la fois
    default_args={"retries": 0},       # pas de relance automatique : chaque appel consomme du quota
    tags=["foot-lab", "landing", "api_football", "open_meteo"],
) as dag:

    # 2.1 Les matchs : la commande que tu tapes à la main, version conteneur
    fixtures = BashOperator(
        task_id="api_football_fixtures",
        bash_command=f"{PYTHON} {INGESTION}/api_football/api_football_fixtures_l_loader.py",
    )

    # 2.2 Branche API-Football : les 4 classements EN FILE après fixtures.
    #     « precedente » = la DERNIÈRE tâche créée : chaque nouvelle tâche est reliée à elle.
    precedente = fixtures
    for table in TABLES_TOP:
        tache = BashOperator(
            task_id=f"api_football_{table}",
            bash_command=f"{PYTHON} {INGESTION}/api_football/api_football_top_l_loader.py --table {table}",
        )
        precedente >> tache          # « >> » = « puis » : la tâche part quand la précédente est verte
        precedente = tache

    # Résultat : fixtures >> topscorers >> topassists >> topyellowcards >> topredcards

    # 2.3 Branche Open-Meteo : géocodage puis météo, AUSSI après fixtures (en parallèle des top*)
    geocodage = BashOperator(
        task_id="open_meteo_geocodage",
        bash_command=f"{PYTHON} {INGESTION}/open_meteo/open_meteo_geocodage_l_loader.py",
    )
    meteo_journaliere = BashOperator(
        task_id="open_meteo_meteo_journaliere",
        bash_command=f"{PYTHON} {INGESTION}/open_meteo/open_meteo_meteo_journaliere_l_loader.py",
    )

    fixtures >> geocodage >> meteo_journaliere   # villes des matchs -> coordonnées -> météo

    # -----------------------------------------------------------------
    # MÊME DAG, écrit SANS boucle (pour mémoire, désactivé) :
    # -----------------------------------------------------------------
    # topscorers = BashOperator(
    #     task_id="api_football_topscorers",
    #     bash_command=f"{PYTHON} {INGESTION}/api_football/api_football_top_l_loader.py --table topscorers",
    # )
    # topassists = BashOperator(
    #     task_id="api_football_topassists",
    #     bash_command=f"{PYTHON} {INGESTION}/api_football/api_football_top_l_loader.py --table topassists",
    # )
    # topyellowcards = BashOperator(
    #     task_id="api_football_topyellowcards",
    #     bash_command=f"{PYTHON} {INGESTION}/api_football/api_football_top_l_loader.py --table topyellowcards",
    # )
    # topredcards = BashOperator(
    #     task_id="api_football_topredcards",
    #     bash_command=f"{PYTHON} {INGESTION}/api_football/api_football_top_l_loader.py --table topredcards",
    # )
    #
    # # L'ordre : « >> » = « puis ». Une seule tâche à la fois (limite d'appels par minute de l'API).
    # fixtures >> topscorers >> topassists >> topyellowcards >> topredcards
    #
    # ⚠️ À NE PAS faire : « fixtures >> tache » dans la boucle -> les 4 classements partiraient
    #    EN MÊME TEMPS après fixtures (appels en parallèle, refus de l'API).