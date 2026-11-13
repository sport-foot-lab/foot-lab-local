"""
=====================================================================
 landing_vers_bronze.py — Landing locale (parquet) -> schéma BRONZE
=====================================================================
 RÔLE
   Équivalent local du notebook bronze du Data Lab (COPY INTO) :
   prendre les fichiers parquet de la landing et les faire entrer
   dans des TABLES de la base, schéma « bronze ».

 CE QUE FAIT LE SCRIPT
   1. lit les parquet de  data/landing/<source>/<table>/<aaaa>/<mm>/<jj>/
   2. charge chaque table dans  bronze.<source>_<table>
      (ex. data/landing/api_football/fixtures/...  ->  bronze.api_football_fixtures)
   3. ajoute 3 colonnes techniques : _date_depot, _fichier_source, _date_chargement_bronze
   4. ne lit chaque fichier qu'UNE seule fois -> le relancer ne crée pas de doublons
   5. garde tous les dépôts (pas de dédoublonnage en bronze : c'est le travail de dbt)

 OÙ VONT LES DONNÉES ?  (même code, seule la destination change)
   --cible dev  -> DuckDB   : un simple FICHIER  data/dev.duckdb  (créé automatiquement)
   --cible prod -> Postgres : la base « datalab » dans Docker
                              (adresse lue dans la variable d'environnement PG_PROD_URL)

 COMMENT LE LANCER  (depuis la racine du dépôt, (env) activé)
   python ingestion/commun/landing_vers_bronze.py --cible dev
   python ingestion/commun/landing_vers_bronze.py --cible dev --sources api_football
   python ingestion/commun/landing_vers_bronze.py --cible dev --reinitialiser
=====================================================================
"""

# ---------------------------------------------------------------------
# PARTIE 0 — Les bibliothèques utilisées
# ---------------------------------------------------------------------
import argparse           # lire les options tapées après le nom du script (--cible, --sources…)
import datetime as dt     # manipuler des dates et heures
import os                 # lire les variables d'environnement (ex. PG_PROD_URL)
import pathlib            # manipuler des chemins de fichiers et de dossiers
import sys                # arrêter proprement le script en cas de problème (sys.exit)

import dlt                # dlthub : le moteur de chargement vers DuckDB / Postgres
import pyarrow as pa      # pyarrow : tableaux de données en mémoire (format natif du parquet)
import pyarrow.parquet as pq  # lecture des fichiers parquet
from dotenv import load_dotenv  # python-dotenv : lit le fichier .env et le transforme en variables d'environnement


# Dossier racine du dépôt (H:\projet-aws\foot-lab-local).
# __file__ = ce fichier ; .parents[2] remonte de 2 niveaux : commun -> ingestion -> racine.
# Grâce à ça, le script trouve data\landing quel que soit le dossier d'où on le lance.
RACINE = pathlib.Path(__file__).resolve().parents[2]
load_dotenv(RACINE / ".env")

# ---------------------------------------------------------------------
# PARTIE 1 — Les options de lancement (--cible, --sources, …)
# ---------------------------------------------------------------------
def lire_arguments():
    """Lit les options tapées dans le terminal. Chaque option a une valeur par défaut."""
    p = argparse.ArgumentParser(description="Landing locale (parquet) -> bronze (DuckDB ou Postgres)")
    # Où charger : dev (DuckDB) ou prod (Postgres). Par défaut : dev.
    p.add_argument("--cible", choices=["dev", "prod"], default="dev",
                   help="dev = DuckDB (défaut), prod = Postgres")
    # Quelles sources charger (noms des dossiers dans data/landing).
    p.add_argument("--sources", default="api_football,open_meteo",
                   help="sources à charger, séparées par des virgules")
    # Où est la landing : 1) option --landing si on la tape,
    # 2) sinon variable d'environnement LANDING_DIR (définie dans le docker-compose pour Airflow),
    # 3) sinon <racine>/data/landing (cas du PC). Même règle que les loaders (outils_landing.py).
    p.add_argument("--landing", default=os.environ.get("LANDING_DIR", str(RACINE / "data" / "landing")),
                   help="dossier racine de la landing locale")
    # Où est (ou sera créé) le fichier DuckDB de dev (par défaut : <racine>/data/dev.duckdb).
    p.add_argument("--duckdb", default=str(RACINE / "data" / "dev.duckdb"),
                   help="fichier DuckDB de dev")
    # Option de remise à zéro : vide les tables bronze concernées puis recharge tout.
    p.add_argument("--reinitialiser", action="store_true",
                   help="vide les tables bronze de ces sources et recharge tous les fichiers")
    return p.parse_args()


# ---------------------------------------------------------------------
# PARTIE 2 — Trouver les tables d'une source
# ---------------------------------------------------------------------
def lister_tables(dossier_source: pathlib.Path):
    """
    Renvoie les dossiers de TABLES d'une source, ex. pour api_football :
    fixtures, topassists, topredcards, topscorers, topyellowcards.
    On ignore les dossiers techniques de dlthub copiés depuis Databricks :
    ceux qui commencent par « _ » (_dlt_version, _dlt_loads…) et « init ».
    """
    return sorted(d for d in dossier_source.iterdir()
                  if d.is_dir() and not d.name.startswith("_") and d.name != "init")


# ---------------------------------------------------------------------
# PARTIE 3 — Ajouter les colonnes techniques à un fichier lu
# ---------------------------------------------------------------------
def ajouter_colonnes_techniques(tableau: pa.Table, fichier: pathlib.Path, landing: pathlib.Path) -> pa.Table:
    """Reçoit le contenu d'UN fichier parquet et lui ajoute les 3 colonnes techniques."""
    n = tableau.num_rows  # nombre de lignes du fichier

    # _date_depot : date à laquelle le fichier a été déposé dans la landing.
    # Elle vient de la colonne _dlt_load_id (écrite par dlthub côté Databricks),
    # qui contient un nombre de secondes depuis le 01/01/1970 -> on le convertit en date.
    if "_dlt_load_id" in tableau.column_names:
        secondes = [float(v) if v is not None else None for v in tableau.column("_dlt_load_id").to_pylist()]
        dates = [dt.datetime.fromtimestamp(s, tz=dt.timezone.utc) if s is not None else None for s in secondes]
    else:
        dates = [None] * n
    tableau = tableau.append_column("_date_depot", pa.array(dates, pa.timestamp("us", tz="UTC")))

    # _fichier_source : chemin du fichier lu, relatif à la landing
    # (ex. api_football/fixtures/2026/10/29/1793293857.343138.72ed459e26.parquet).
    chemin = fichier.relative_to(landing).as_posix()
    tableau = tableau.append_column("_fichier_source", pa.array([chemin] * n, pa.string()))

    # _date_chargement_bronze : moment où ce script charge la ligne dans le bronze.
    maintenant = dt.datetime.now(dt.timezone.utc)
    tableau = tableau.append_column("_date_chargement_bronze", pa.array([maintenant] * n, pa.timestamp("us", tz="UTC")))

    # Colonnes entièrement vides dans CE fichier : leur type est inconnu (« null »),
    # ce qui gênerait la création de la table. On les retire pour ce fichier seulement :
    # elles seront créées avec le bon type dès qu'un autre fichier les remplira.
    vides = [c.name for c in tableau.schema if pa.types.is_null(c.type)]
    if vides:
        tableau = tableau.drop_columns(vides)
    return tableau


# ---------------------------------------------------------------------
# PARTIE 4 — Une « ressource » dlthub par table (le cœur du chargement)
# ---------------------------------------------------------------------
def ressource_table(source: str, dossier_table: pathlib.Path, landing: pathlib.Path):
    """
    Une RESSOURCE dlthub = « une table à charger ».
    Elle lit les fichiers parquet d'un dossier de table et les envoie à dlthub,
    qui les écrit dans bronze.<source>_<table>.
    """
    nom_table = f"{source}_{dossier_table.name}"   # ex. api_football_fixtures

    # write_disposition="append" : on AJOUTE les lignes à la table (on ne remplace rien).
    @dlt.resource(name=nom_table, write_disposition="append")
    def lire_fichiers():
        # L'« état » dlthub est une petite mémoire conservée entre deux lancements.
        # On y garde la liste des fichiers déjà chargés pour ne jamais les relire.
        etat = dlt.current.resource_state()
        deja_lus = set(etat.setdefault("fichiers_lus", []))

        # Tous les parquet du dossier de la table, sous-dossiers compris (rglob = recherche récursive).
        fichiers = sorted(dossier_table.rglob("*.parquet"))
        # On garde seulement ceux qui n'ont jamais été chargés.
        nouveaux = [f for f in fichiers if f.relative_to(landing).as_posix() not in deja_lus]
        print(f"  {nom_table:<40} fichiers : {len(fichiers):>3} | nouveaux : {len(nouveaux):>3}")

        for f in nouveaux:
            tableau = pq.read_table(f)                                   # lire le fichier parquet
            yield ajouter_colonnes_techniques(tableau, f, landing)       # l'envoyer à dlthub
            etat["fichiers_lus"].append(f.relative_to(landing).as_posix())  # le noter comme « lu »

    return lire_fichiers


# ---------------------------------------------------------------------
# PARTIE 5 — Choisir la destination : DuckDB (dev) ou Postgres (prod)
# ---------------------------------------------------------------------
def destination(args):
    if args.cible == "dev":
        # DEV = DuckDB : la base est UN FICHIER (data/dev.duckdb).
        # S'il n'existe pas, il est créé automatiquement au premier chargement.
        pathlib.Path(args.duckdb).parent.mkdir(parents=True, exist_ok=True)
        return dlt.destinations.duckdb(credentials=args.duckdb)

    # PROD = Postgres : l'adresse de connexion (avec le mot de passe) n'est JAMAIS écrite
    # dans le code ; elle est lue dans la variable d'environnement PG_PROD_URL (fichier .env).
    url = os.environ.get("PG_PROD_URL")
    if not url:
        sys.exit("❌ Variable d'environnement PG_PROD_URL absente (voir .env.example).")
    return dlt.destinations.postgres(credentials=url)


# ---------------------------------------------------------------------
# PARTIE 6 — Le programme principal : tout enchaîner
# ---------------------------------------------------------------------
def main():
    # 6.1 Lire les options et préparer les chemins
    args = lire_arguments()
    landing = pathlib.Path(args.landing).resolve()
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]

    # 6.2 Construire la liste des tables à charger (une ressource par dossier de table)
    ressources = []
    for source in sources:
        dossier_source = landing / source
        if not dossier_source.is_dir():
            sys.exit(f"❌ Dossier introuvable : {dossier_source}")
        for dossier_table in lister_tables(dossier_source):
            ressources.append(ressource_table(source, dossier_table, landing))

    # 6.3 Créer le pipeline dlthub
    #     - pipeline_name : nom du pipeline (un par cible : landing_bronze_dev / landing_bronze_prod)
    #     - destination   : DuckDB ou Postgres (partie 5)
    #     - dataset_name  : le SCHÉMA où sont créées les tables -> « bronze »
    pipeline = dlt.pipeline(
        pipeline_name=f"landing_bronze_{args.cible}",
        destination=destination(args),
        dataset_name="bronze",
    )
    print(f"Cible : {args.cible} | landing : {landing}")

    # 6.4 Lancer le chargement. Avec --reinitialiser, dlthub vide d'abord les tables concernées.
    info = pipeline.run(ressources, refresh="drop_resources" if args.reinitialiser else None)
    info.raise_on_failed_jobs()   # s'arrêter en erreur si un morceau du chargement a échoué
    print(info)                   # résumé dlthub (« Load package … is LOADED … »)

    # 6.5 Contrôle : compter les lignes de chaque table bronze après chargement
    print("\nLignes par table bronze :")
    with pipeline.sql_client() as client:
        for r in ressources:
            nom = r.name
            try:
                n = client.execute_sql(f"SELECT COUNT(*) FROM {client.make_qualified_table_name(nom)}")[0][0]
            except Exception:
                n = "table absente (aucun fichier chargé)"
            print(f"  bronze.{nom:<40} {n}")


# Point d'entrée : ce bloc s'exécute quand on lance « python landing_vers_bronze.py ».
if __name__ == "__main__":
    main()
