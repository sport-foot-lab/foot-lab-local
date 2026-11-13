"""
=====================================================================
 api_football_top_l_loader.py — API-Football : classements joueurs -> landing
=====================================================================
 Traduction locale des 4 notebooks Data Lab « api_football_top*_l_loader »
 (topscorers, topassists, topyellowcards, topredcards) : un seul script,
 la table est choisie avec --table.

 RÔLE
   Appeler /players/<table> pour chaque couple championnat × saison
   (20 joueurs par appel) et déposer les lignes en parquet dans :
     data/landing/api_football/<table>/<aaaa>/<mm>/<jj>/<load_id>.<file_id>.parquet

 ÉCONOMIE DE QUOTA
   Un couple n'est PAS rappelé s'il est à la fois :
     1) déjà présent dans la landing de cette table, ET
     2) terminé d'après le dernier dépôt fixtures (tous les matchs ont un statut final).
   -> à lancer APRÈS api_football_fixtures_l_loader.py.

 LANCEMENT (depuis la racine du dépôt, (env) activé)
   python ingestion/api_football/api_football_top_l_loader.py --table topscorers
   python ingestion/api_football/api_football_top_l_loader.py --table topassists --leagues 61 --seasons 2024
=====================================================================
"""

# ---------------------------------------------------------------------
# PARTIE 0 — Bibliothèques, boîte à outils, et fonctions réutilisées du loader fixtures
# ---------------------------------------------------------------------
import argparse
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))   # dossier « ingestion »

import dlt
from commun.outils_landing import (
    appeler_api, arreter_si_rien, charger, liste_entiers,
    lire_cle_api, pipeline_landing, requete_landing,
)
# On RÉUTILISE deux fonctions du loader fixtures (même source, mêmes règles) au lieu de les recopier :
#   - couples_deja_complets() : saisons terminées d'après le dernier dépôt fixtures
#   - verifier_quota()        : contrôle du quota /status
from api_football.api_football_fixtures_l_loader import (
    BASE_URL, PAUSE_SECONDES, SOURCE, couples_deja_complets, verifier_quota,
)


# ---------------------------------------------------------------------
# PARTIE 1 — Constantes
# ---------------------------------------------------------------------
TABLES_AUTORISEES = ["topscorers", "topassists", "topyellowcards", "topredcards"]

# Colonnes toujours vides chez ces joueurs (avertissement dlthub du Data Lab) :
# on impose leur type pour qu'elles existent quand même (sinon dlthub ne les crée pas).
# Entier = même type que leurs colonnes sœurs remplies.
TYPES_IMPOSES = {
    "statistics__dribbles__past":    {"data_type": "bigint", "nullable": True},
    "statistics__games__number":     {"data_type": "bigint", "nullable": True},   # numéro de maillot
    "statistics__goals__saves":      {"data_type": "bigint", "nullable": True},
    "statistics__penalty__commited": {"data_type": "bigint", "nullable": True},   # orthographe de l'API (un seul t)
    "statistics__penalty__saved":    {"data_type": "bigint", "nullable": True},
    "statistics__penalty__won":      {"data_type": "bigint", "nullable": True},
}


# ---------------------------------------------------------------------
# PARTIE 2 — Les options (--table + les widgets LEAGUE_IDS / SEASONS)
# ---------------------------------------------------------------------
def lire_arguments():
    p = argparse.ArgumentParser(description="API-Football : classements joueurs (top*) -> landing")
    p.add_argument("--table", required=True, choices=TABLES_AUTORISEES, help="classement à charger")
    p.add_argument("--leagues", default="61,39,78,140,135,2", help="ids des championnats")
    p.add_argument("--seasons", default="2022,2023,2024", help="saisons")
    return p.parse_args()


# ---------------------------------------------------------------------
# PARTIE 3 — Couples déjà chargés dans la landing de CETTE table
# ---------------------------------------------------------------------
def couples_deja_charges(table: str) -> set[tuple[int, int]]:
    """Couples (championnat, saison) déjà présents : colonnes _param_ = paramètres exacts de l'appel."""
    lignes = requete_landing("""
        SELECT DISTINCT _param_league AS league, _param_season AS season FROM {top}
    """, top=(SOURCE, table))
    return {(int(r["league"]), int(r["season"])) for r in lignes}


# ---------------------------------------------------------------------
# PARTIE 4 — Découper la réponse de l'API en lignes
# ---------------------------------------------------------------------
def extraire_lignes(reponse: dict) -> list[dict]:
    """
    Chaque joueur a une liste « statistics » : 1 élément par CLUB du joueur dans la compétition × saison
    (2 s'il a été transféré en cours de saison). On fait 1 ligne par joueur × club :
    dlthub aplatit alors statistics en colonnes statistics__… (pas de table enfant).
    """
    lignes = []
    for joueur in reponse["response"]:
        stats = joueur.get("statistics") or []
        if not stats:                                       # garde-fou : réponse anormale
            raise RuntimeError(f"statistics vide (joueur {joueur['player']['id']})")
        for stat in stats:
            lignes.append({**joueur, "statistics": stat})   # même joueur, un club à la fois
    return lignes


# ---------------------------------------------------------------------
# PARTIE 5 — La ressource dlthub
# ---------------------------------------------------------------------
def ressource_top(table: str, appels: list[dict], headers: dict):
    @dlt.resource(name=table, write_disposition="append", columns=TYPES_IMPOSES)
    def top():
        for i, params in enumerate(appels):
            if i > 0:
                time.sleep(PAUSE_SECONDES)
            reponse = appeler_api(f"{BASE_URL}/players/{table}", params, headers)
            if reponse.get("errors"):
                raise RuntimeError(f"Erreur API-Football ({params}) : {reponse['errors']}")
            lignes = extraire_lignes(reponse)
            print(f"📥 {params} : {len(lignes)} ligne(s)")
            # On ajoute à chaque ligne les paramètres de l'appel : _param_league, _param_season
            yield [{**ligne, **{f"_param_{k}": v for k, v in params.items()}} for ligne in lignes]
    return top


# ---------------------------------------------------------------------
# PARTIE 6 — Programme principal
# ---------------------------------------------------------------------
def main():
    args = lire_arguments()
    table = args.table
    leagues, seasons = liste_entiers(args.leagues), liste_entiers(args.seasons)

    # 6.1 Appels demandés, moins ceux déjà chargés ET terminés
    demandes = [(l, s) for l in leagues for s in seasons]
    a_exclure = couples_deja_charges(table) & couples_deja_complets()
    appels = [{"league": l, "season": s} for (l, s) in demandes if (l, s) not in a_exclure]
    print(f"Table {table} | demandés : {len(demandes)} | déjà chargés et terminés : {len(a_exclure)} "
          f"| à appeler : {len(appels)}")
    arreter_si_rien(appels, f"{table} : tous les couples demandés sont déjà en landing et terminés.")

    # 6.2 Clé API et quota
    headers = {"x-apisports-key": lire_cle_api("API_FOOTBALL_KEY")}
    verifier_quota(headers, len(appels))

    # 6.3 Chargement API -> landing
    pipeline = pipeline_landing(SOURCE, table)
    charger(pipeline, ressource_top(table, appels, headers))

    # 6.4 Vérification : lignes déposées par CE chargement
    load_id = pipeline.last_trace.last_load_info.loads_ids[-1]
    print(f"\nLignes {table} déposées par ce chargement :")
    for r in requete_landing(f"""
        SELECT _param_league AS league, _param_season AS season, COUNT(*) AS nb
        FROM {{top}} WHERE _dlt_load_id = '{load_id}'
        GROUP BY ALL ORDER BY league, season
    """, top=(SOURCE, table)):
        print(f"  ligue {r['league']:>4} | saison {r['season']} : {r['nb']} ligne(s)")


if __name__ == "__main__":
    main()