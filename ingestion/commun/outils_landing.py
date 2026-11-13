"""
=====================================================================
 outils_landing.py — Boîte à outils commune des loaders API -> landing
=====================================================================
 RÔLE
   Regrouper ce dont TOUS les loaders ont besoin, pour ne pas le recopier
   dans chaque fichier (équivalent de utils_orchestration dans le Data Lab).

 UTILISÉ PAR
   ingestion/api_football/api_football_fixtures_l_loader.py
   ingestion/api_football/api_football_top_l_loader.py
   ingestion/open_meteo/open_meteo_geocodage_l_loader.py
   ingestion/open_meteo/open_meteo_meteo_journaliere_l_loader.py

 CE FICHIER NE SE LANCE PAS SEUL : les loaders l'importent avec
   from commun.outils_landing import ...
=====================================================================
"""

# ---------------------------------------------------------------------
# PARTIE 0 — Bibliothèques
# ---------------------------------------------------------------------
import os                 # variables d'environnement (clé API…)
import pathlib            # chemins de fichiers et de dossiers
import sys                # arrêter proprement le script (sys.exit)

import duckdb             # lire les parquet de la landing avec du SQL (remplace Spark)
import dlt                # dlthub : écrire les réponses de l'API en parquet dans la landing
from dlt.sources.helpers import requests   # « requests » avec relances automatiques en cas d'erreur réseau
from dotenv import load_dotenv             # lire le fichier .env (clé API)


# ---------------------------------------------------------------------
# PARTIE 1 — Les chemins du projet et la lecture du .env
# ---------------------------------------------------------------------
# Racine du dépôt (H:\projet-aws\foot-lab-local) : ce fichier est dans ingestion/commun/ -> on remonte de 2 niveaux.
RACINE = pathlib.Path(__file__).resolve().parents[2]

# Lecture du fichier .env de la racine : ses lignes NOM=valeur deviennent des variables d'environnement.
# override=False : une variable déjà définie (par ex. par Airflow plus tard) n'est PAS écrasée.
load_dotenv(RACINE / ".env", override=False)

# Pas d'envoi de statistiques d'usage à dlthub (comme dans le Data Lab).
os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")

# Dossier de la landing : data/landing par défaut.
# La variable LANDING_DIR permettra plus tard à Airflow (dans Docker) d'indiquer un autre chemin.
LANDING = pathlib.Path(os.environ.get("LANDING_DIR", RACINE / "data" / "landing")).resolve()

# Rangement des fichiers dans la landing : <table>/<aaaa>/<mm>/<jj>/<load_id>.<file_id>.parquet
# (convention figée du Data Lab ; la date = jour du DÉPÔT, pas la date des matchs).
LAYOUT = "{table_name}/{YYYY}/{MM}/{DD}/{load_id}.{file_id}.{ext}"


# ---------------------------------------------------------------------
# PARTIE 2 — Lire la clé d'une API (remplace dbutils.secrets.get)
# ---------------------------------------------------------------------
def lire_cle_api(nom_variable: str) -> str:
    """
    Renvoie la clé rangée dans le .env sous le nom donné (ex. API_FOOTBALL_KEY).
    S'arrête avec un message clair si elle est absente ou encore à sa valeur d'exemple.
    La clé n'est JAMAIS affichée.
    """
    cle = os.environ.get(nom_variable, "").strip()
    if not cle or cle == "ta_cle_ici":
        sys.exit(f"❌ {nom_variable} absente du fichier .env (voir .env.example).")
    return cle


# ---------------------------------------------------------------------
# PARTIE 3 — Petites aides
# ---------------------------------------------------------------------
def liste_entiers(texte: str) -> list[int]:
    """'61, 39,78' -> [61, 39, 78] (comme dans les notebooks du Data Lab)."""
    return [int(x) for x in texte.split(",") if x.strip()]


def liste_textes(texte: str) -> list[str]:
    """'Lyon, Nantes' -> ['Lyon', 'Nantes'] ; texte vide -> []."""
    return [x.strip() for x in texte.split(",") if x.strip()]


def appeler_api(url: str, params: dict, headers: dict | None = None) -> dict:
    """Fait UN appel GET à l'API et renvoie la réponse JSON (avec relances automatiques si le réseau flanche)."""
    return requests.get(url, params=params, headers=headers or {}, timeout=60).json()


def arreter_si_rien(appels: list, message: str) -> None:
    """
    S'il n'y a aucun appel à faire : on le dit et on s'arrête EN SUCCÈS (code 0).
    Remplace dbutils.notebook.exit(...) : pour Airflow, la tâche sera verte.
    """
    if not appels:
        print(f"✅ Rien à charger : {message}")
        sys.exit(0)


# ---------------------------------------------------------------------
# PARTIE 4 — Interroger ce qui est DÉJÀ en landing (remplace spark.read.parquet)
# ---------------------------------------------------------------------
def chemin_table(source: str, table: str) -> pathlib.Path:
    """Dossier d'une table dans la landing, ex. data/landing/api_football/fixtures."""
    return LANDING / source / table


def fichiers_table(source: str, table: str) -> list[pathlib.Path]:
    """Tous les parquet d'une table de la landing (sous-dossiers <aaaa>/<mm>/<jj> compris)."""
    dossier = chemin_table(source, table)
    return sorted(dossier.glob("*/*/*/*.parquet")) if dossier.is_dir() else []


def requete_landing(sql: str, **tables: tuple[str, str]) -> list[dict]:
    """
    Exécute une requête SQL DuckDB sur les parquet de la landing et renvoie les lignes (liste de dict).

    Dans le SQL, chaque table s'écrit {nom} ; on indique à quoi correspond chaque nom :
        requete_landing("SELECT DISTINCT league__id FROM {fx}", fx=("api_football", "fixtures"))

    Si une des tables n'a encore AUCUN fichier (1er lancement), on renvoie [] au lieu de planter
    (remplace le try/except PATH_NOT_FOUND des notebooks).
    """
    remplacements = {}
    for nom, (source, table) in tables.items():
        if not fichiers_table(source, table):
            print(f"ℹ️ Aucun fichier en landing pour {source}/{table} (1er lancement ou dossier vidé)")
            return []
        motif = (chemin_table(source, table) / "*" / "*" / "*" / "*.parquet").as_posix()
        # union_by_name : réunit les colonnes de tous les fichiers, même si l'un en a une de plus
        remplacements[nom] = f"read_parquet('{motif}', union_by_name = true)"

    con = duckdb.connect()            # base DuckDB en mémoire, juste le temps de la requête
    try:
        resultat = con.execute(sql.format(**remplacements))
        colonnes = [c[0] for c in resultat.description]
        return [dict(zip(colonnes, ligne)) for ligne in resultat.fetchall()]
    finally:
        con.close()


# ---------------------------------------------------------------------
# PARTIE 5 — Le pipeline dlthub qui écrit dans la landing
# ---------------------------------------------------------------------
def pipeline_landing(source: str, table: str):
    """
    Pipeline dlthub « API -> fichiers parquet dans data/landing/<source>/<table>/<aaaa>/<mm>/<jj>/ ».
    - destination filesystem = un dossier (ici sur ton disque) ;
    - dataset_name = le sous-dossier de la source (api_football, open_meteo).
    """
    LANDING.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name=f"{source}_{table}",
        destination=dlt.destinations.filesystem(bucket_url=LANDING.as_uri(), layout=LAYOUT),
        dataset_name=source,
    )


def charger(pipeline, ressource) -> None:
    """Lance le chargement en parquet, affiche le résumé dlthub et s'arrête en erreur si un fichier n'a pas pu être écrit."""
    info = pipeline.run(ressource, loader_file_format="parquet")
    print(info)
    info.raise_on_failed_jobs()
