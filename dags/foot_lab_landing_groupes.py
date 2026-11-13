"""
=====================================================================
 foot_lab_landing_groupes.py — DAG Airflow : API -> landing -> bronze (dev + prod)
=====================================================================
 RÔLE
   Lancer, dans l'ordre, les loaders qu'on lance aussi à la main, rangés en 3 GROUPES :
     groupe api_football : fixtures > topscorers > topassists > topyellowcards > topredcards
     groupe open_meteo   : geocodage > meteo_journaliere
     groupe bronze       : dev (DuckDB) + prod (Postgres), en parallèle, après les 2 groupes
   Le groupe open_meteo part dès que api_football.fixtures est vert
   (il a besoin des villes des matchs, pas des classements) : les 2 API travaillent en parallèle.
   Le groupe bronze attend que TOUTE la landing soit à jour (les 2 groupes verts).

 AFFICHAGE
   Graph : 3 blocs repliés ; un clic sur un bloc le déplie et montre ses tâches.
   Nom complet d'une tâche = <groupe>.<tâche>, ex. api_football.fixtures, bronze.prod.

 PRINCIPE
   Chaque tâche = la MÊME commande que dans le terminal VS Code, mais lancée
   dans le conteneur Airflow avec l'environnement Python des loaders
   (/opt/airflow/ingestion_venv, mêmes versions que le PC).
   Script terminé proprement (exit 0, y compris « Rien à charger ») -> tâche VERTE.
   Erreur (quota, clé, API, fichier verrouillé) -> tâche ROUGE, les suivantes de SA branche ne partent pas.

 DEV ET PROD ISO
   Une seule landing (data/landing), commune aux 2 environnements.
   Le bronze est chargé par le MÊME script (commun/landing_vers_bronze.py), seule --cible change :
     bronze.dev  -> DuckDB  : /opt/airflow/data/dev.duckdb (= data/dev.duckdb sur le PC)
     bronze.prod -> Postgres : base datalab (adresse dans PG_PROD_URL, variable du docker-compose)
   ⚠️ DuckDB UI FERMÉE pendant les exécutions (un seul écrivain DuckDB) :
      sinon bronze.dev est ROUGE (rien d'abîmé, l'exécution suivante rattrape).

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

# Les 2 environnements du bronze : même script, seule la destination change
CIBLES_BRONZE = ["dev", "prod"]


# ---------------------------------------------------------------------
# PARTIE 2 — Le DAG
# ---------------------------------------------------------------------
with DAG(
    dag_id="foot_lab_landing_groupes",
    description="API -> landing -> bronze : API-Football + Open-Meteo, bronze dev (DuckDB) et prod (Postgres)",
    schedule="0 */2 * * *",            # toutes les 2 heures, à l'heure pile (cron, heure UTC)
    # schedule="@daily",               # plus tard : 1 fois par jour (à minuit UTC)
    start_date=datetime(2026, 11, 1),
    catchup=False,                     # ne pas rattraper les dates passées
    max_active_runs=1,                 # une seule exécution à la fois
    default_args={"retries": 0},       # pas de relance automatique : chaque appel consomme du quota
    tags=["foot-lab", "landing", "bronze", "api_football", "open_meteo"],
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

    # -----------------------------------------------------------------
    # 2.4 GROUPE bronze : landing -> bronze DEV et PROD, une fois les 2 groupes terminés
    #     MÊME script, MÊME logique ; seule la destination change (--cible) -> dev et prod sont ISO.
    #     Pas de --sources : valeur par défaut "api_football,open_meteo" = les 2 sources.
    #     Chaque cible ne lit que les fichiers qu'ELLE n'a jamais chargés -> rien de nouveau = tâche verte.
    # -----------------------------------------------------------------
    with TaskGroup(group_id="bronze", tooltip="Landing -> bronze : DEV (DuckDB) et PROD (Postgres)") as groupe_bronze:

        # Une tâche par cible, créées par une boucle : même commande, seul --cible change.
        # Aucune flèche entre elles -> elles partent EN MÊME TEMPS et sont indépendantes
        # (un échec en dev ne bloque pas la prod, et inversement).
        for cible in CIBLES_BRONZE:
            BashOperator(
                task_id=cible,                                     # nom complet : bronze.dev / bronze.prod
                bash_command=f"{PYTHON} {INGESTION}/commun/landing_vers_bronze.py --cible {cible}",
            )

    # Une LISTE à gauche de « >> » = « quand TOUS ces éléments sont verts, puis... ».
    # Le groupe bronze attend les 5 tâches foot ET les 2 tâches météo.
    # Si une seule est rouge, aucun bronze n'est chargé (règle Airflow par défaut : tout doit être vert).
    [groupe_foot, groupe_meteo] >> groupe_bronze