"""
=====================================================================
 foot_lab_landing.py — DAG Airflow : API -> landing (API-Football + Open-Meteo)
=====================================================================
 RÔLE
   Lancer, dans l'ordre, les loaders qu'on lance aussi à la main, rangés en 2 GROUPES :
     groupe api_football : fixtures > topscorers > topassists > topyellowcards > topredcards
     groupe open_meteo   : geocodage > meteo_journaliere
   Le groupe open_meteo part dès que api_football.fixtures est vert
   (il a besoin des villes des matchs, pas des classements) : les 2 API travaillent en parallèle.

 AFFICHAGE
   Graph : 2 blocs repliés ; un clic sur un bloc le déplie et montre ses tâches.
   Nom complet d'une tâche = <groupe>.<tâche>, ex. api_football.fixtures.

 PRINCIPE
   Chaque tâche = la MÊME commande que dans le terminal VS Code, mais lancée
   dans le conteneur Airflow avec l'environnement Python des loaders
   (/opt/airflow/ingestion_venv, mêmes versions que le PC).
   Script terminé proprement (exit 0, y compris « Rien à charger ») -> tâche VERTE.
   Erreur (quota, clé, API) -> tâche ROUGE, les suivantes de SA branche ne partent pas.

 PLANIFICATION
   schedule="0 */2 * * *" : toutes les 2 heures (test). Plus tard : "@daily" (1 fois par jour).
   Si tout est déjà complet en landing, chaque exécution s'arrête sur « Rien à charger » : 0 appel API.
   Météo : 2 000 appels max par exécution -> les gros rattrapages se font en plusieurs exécutions.
=====================================================================
"""

from datetime import datetime

from airflow.sdk import DAG, TaskGroup                              # Airflow 3 : DAG + groupes de tâches
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
    dag_id="foot_lab_landing_groupes",
    description="API -> landing : API-Football (fixtures, top*) + Open-Meteo (géocodage, météo)",
    schedule="0 */2 * * *",            # toutes les 2 heures, à l'heure pile (cron, heure UTC)
    # schedule="@daily",               # plus tard : 1 fois par jour (à minuit UTC)
    start_date=datetime(2026, 11, 1),
    catchup=False,                     # ne pas rattraper les dates passées
    max_active_runs=1,                 # une seule exécution à la fois
    default_args={"retries": 0},       # pas de relance automatique : chaque appel consomme du quota
    tags=["foot-lab", "landing", "api_football", "open_meteo"],
) as dag:

    # -----------------------------------------------------------------
    # 2.1 GROUPE api_football : les matchs, puis les 4 classements EN FILE
    #     Tout ce qui est créé DANS le « with TaskGroup » appartient au groupe.
    # -----------------------------------------------------------------
    with TaskGroup(group_id="api_football", tooltip="API-Football : matchs + 4 classements joueurs") as groupe_foot:

        fixtures = BashOperator(
            task_id="fixtures",                                   # nom complet : api_football.fixtures
            bash_command=f"{PYTHON} {INGESTION}/api_football/api_football_fixtures_l_loader.py",
        )

        # « precedente » = la DERNIÈRE tâche créée : chaque nouvelle tâche est reliée à elle (file d'attente)
        precedente = fixtures
        for table in TABLES_TOP:
            tache = BashOperator(
                task_id=table,                                    # ex. api_football.topscorers
                bash_command=f"{PYTHON} {INGESTION}/api_football/api_football_top_l_loader.py --table {table}",
            )
            precedente >> tache      # « >> » = « puis » : la tâche part quand la précédente est verte
            precedente = tache

        # Résultat : fixtures >> topscorers >> topassists >> topyellowcards >> topredcards

    # -----------------------------------------------------------------
    # 2.2 GROUPE open_meteo : villes -> coordonnées, puis météo des jours de match
    # -----------------------------------------------------------------
    with TaskGroup(group_id="open_meteo", tooltip="Open-Meteo : géocodage des stades + météo journalière") as groupe_meteo:

        geocodage = BashOperator(
            task_id="geocodage",                                  # nom complet : open_meteo.geocodage
            bash_command=f"{PYTHON} {INGESTION}/open_meteo/open_meteo_geocodage_l_loader.py",
        )
        meteo_journaliere = BashOperator(
            task_id="meteo_journaliere",                          # nom complet : open_meteo.meteo_journaliere
            bash_command=f"{PYTHON} {INGESTION}/open_meteo/open_meteo_meteo_journaliere_l_loader.py",
        )

        geocodage >> meteo_journaliere

    # -----------------------------------------------------------------
    # 2.3 Lien ENTRE les groupes : la météo a besoin des matchs, pas des classements
    #     -> on relie fixtures (dans le groupe foot) à geocodage (dans le groupe météo).
    #     Dans le Graph replié, ça s'affiche comme une flèche api_football -> open_meteo.
    # -----------------------------------------------------------------
    fixtures >> geocodage

    # Variante (désactivée) : relier les GROUPES entiers -> la météo attendrait AUSSI les 4 top*.
    # groupe_foot >> groupe_meteo