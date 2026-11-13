"""
=====================================================================
 api_football_fixtures_l_loader.py — API-Football : matchs -> landing
=====================================================================
 Traduction locale du notebook Data Lab « api_football_fixtures_l_loader ».

 RÔLE
   Appeler l'API-Football (/fixtures) pour chaque couple championnat × saison
   et déposer les matchs en parquet dans :
     data/landing/api_football/fixtures/<aaaa>/<mm>/<jj>/<load_id>.<file_id>.parquet
   (date du chemin = jour du DÉPÔT, pas la date des matchs)

 ÉCONOMIE DE QUOTA (100 requêtes / jour en gratuit)
   - un couple championnat × saison déjà COMPLET en landing n'est pas rappelé
     (tous ses matchs ont un statut final dans son DERNIER dépôt) ;
   - le quota restant est vérifié AVANT tout appel (/status ne compte pas).

 LANCEMENT (depuis la racine du dépôt, (env) activé)
   python ingestion/api_football/api_football_fixtures_l_loader.py
   python ingestion/api_football/api_football_fixtures_l_loader.py --leagues 61 --seasons 2024
=====================================================================
"""

# ---------------------------------------------------------------------
# PARTIE 0 — Bibliothèques et boîte à outils commune
# ---------------------------------------------------------------------
import argparse      # lire les options (--leagues, --seasons) = les widgets du notebook
import pathlib       # chemins
import sys           # arrêts propres
import time          # pause entre deux appels

# Pour pouvoir importer ingestion/commun/outils_landing.py :
# on ajoute le dossier « ingestion » à la liste des endroits où Python cherche les modules.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import dlt
from commun.outils_landing import (
    appeler_api, arreter_si_rien, charger, liste_entiers,
    lire_cle_api, pipeline_landing, requete_landing,
)


# ---------------------------------------------------------------------
# PARTIE 1 — Constantes de la source (identiques à celles du Data Lab)
# ---------------------------------------------------------------------
SOURCE = "api_football"                         # dossier data/landing/api_football
TABLE = "fixtures"                              # dossier data/landing/api_football/fixtures
BASE_URL = "https://v3.football.api-sports.io"  # adresse de l'API
PAUSE_SECONDES = 7                              # pause entre deux appels (limite par minute de l'API)

# Statuts d'un match qui ne bougera plus : terminé, prolongation, tirs au but,
# tapis vert, forfait, annulé, abandonné.
STATUTS_FINAUX = ["FT", "AET", "PEN", "AWD", "WO", "CANC", "ABD"]


# ---------------------------------------------------------------------
# PARTIE 2 — Les options (= les widgets LEAGUE_IDS et SEASONS du notebook)
# ---------------------------------------------------------------------
def lire_arguments():
    p = argparse.ArgumentParser(description="API-Football : matchs (fixtures) -> landing")
    # Ligue 1, Premier League, Bundesliga, Liga, Serie A, Ligue des champions
    p.add_argument("--leagues", default="61,39,78,140,135,2", help="ids des championnats, séparés par des virgules")
    # Année de début de saison ; le plan gratuit donne 2022 à 2024
    p.add_argument("--seasons", default="2022,2023,2024", help="saisons, séparées par des virgules")
    return p.parse_args()


# ---------------------------------------------------------------------
# PARTIE 3 — Couples déjà complets en landing (= cellule « 4 bis » du notebook)
# ---------------------------------------------------------------------
def couples_deja_complets() -> set[tuple[int, int]]:
    """
    Renvoie les couples (championnat, saison) à NE PAS rappeler :
    dans le DERNIER dépôt de ce couple, tous les matchs ont un statut final.
    (On regarde le dernier dépôt seulement : un ancien dépôt « match à venir » ne doit pas bloquer.)
    """
    statuts = ", ".join(f"'{s}'" for s in STATUTS_FINAUX)
    lignes = requete_landing(f"""
        WITH matchs AS (
            SELECT league__id, league__season, fixture__status__short AS statut,
                   CAST(_dlt_load_id AS DOUBLE) AS depot
            FROM {{fx}}
        ),
        dernier AS (                                   -- dernier dépôt de chaque couple
            SELECT league__id, league__season, MAX(depot) AS depot
            FROM matchs GROUP BY league__id, league__season
        )
        SELECT m.league__id, m.league__season
        FROM matchs m JOIN dernier d USING (league__id, league__season, depot)
        GROUP BY m.league__id, m.league__season
        HAVING BOOL_AND(m.statut IN ({statuts}))      -- TOUS les matchs ont un statut final
    """, fx=(SOURCE, TABLE))
    return {(int(r["league__id"]), int(r["league__season"])) for r in lignes}


# ---------------------------------------------------------------------
# PARTIE 4 — Contrôle du quota (/status, gratuit)
# ---------------------------------------------------------------------
def verifier_quota(headers: dict, nb_appels: int) -> None:
    reponse = appeler_api(f"{BASE_URL}/status", {}, headers)
    if reponse.get("errors"):                         # ex. clé invalide
        sys.exit(f"❌ Erreur API-Football (/status) : {reponse['errors']}")
    statut = reponse["response"]
    utilisees = statut["requests"]["current"]
    limite = statut["requests"]["limit_day"]
    restantes = limite - utilisees
    print(f"📊 Plan {statut['subscription']['plan']} | actif={statut['subscription']['active']} "
          f"| {utilisees}/{limite} utilisées | {restantes} restantes")
    if not statut["subscription"]["active"] or restantes < nb_appels:
        sys.exit(f"❌ Quota insuffisant : {nb_appels} requêtes nécessaires, {restantes} restantes. Chargement annulé.")


# ---------------------------------------------------------------------
# PARTIE 5 — La ressource dlthub : les matchs (= cellule « ressource » du notebook)
# ---------------------------------------------------------------------
def ressource_fixtures(couples: list[tuple[int, int]], headers: dict):
    @dlt.resource(name=TABLE, write_disposition="append")   # append : la landing garde tous les dépôts
    def fixtures():
        for i, (league_id, season) in enumerate(couples):
            if i > 0:
                time.sleep(PAUSE_SECONDES)                    # pause entre deux appels
            reponse = appeler_api(f"{BASE_URL}/fixtures", {"league": league_id, "season": season}, headers)
            if reponse.get("errors"):                         # ex. saison non autorisée par le plan
                raise RuntimeError(f"Erreur API-Football (ligue {league_id}, saison {season}) : {reponse['errors']}")
            nom = reponse["response"][0]["league"]["name"] if reponse["response"] else "?"
            print(f"📥 {nom} (id {league_id}) | saison {season} : {reponse['results']} matchs")
            yield reponse["response"]                         # dlthub aplatit : teams.home.name -> teams__home__name
    return fixtures


# ---------------------------------------------------------------------
# PARTIE 6 — Programme principal
# ---------------------------------------------------------------------
def main():
    args = lire_arguments()
    leagues, seasons = liste_entiers(args.leagues), liste_entiers(args.seasons)

    # 6.1 Liste des couples demandés, moins ceux déjà complets en landing
    demandes = [(l, s) for l in leagues for s in seasons]
    complets = couples_deja_complets()
    a_appeler = [c for c in demandes if c not in complets]
    print(f"Demandés : {len(demandes)} | déjà complets (non rappelés) : {sorted(set(demandes) & complets)}")
    print(f"À appeler : {a_appeler} -> {len(a_appeler)} requête(s)")
    arreter_si_rien(a_appeler, "toutes les saisons demandées sont déjà complètes en landing.")

    # 6.2 Clé API (lue dans .env) et contrôle du quota AVANT d'appeler
    headers = {"x-apisports-key": lire_cle_api("API_FOOTBALL_KEY")}
    verifier_quota(headers, len(a_appeler))

    # 6.3 Chargement API -> landing (parquet)
    pipeline = pipeline_landing(SOURCE, TABLE)
    charger(pipeline, ressource_fixtures(a_appeler, headers))

    # 6.4 Vérification : matchs déposés par CE chargement, par championnat et saison
    load_id = pipeline.last_trace.last_load_info.loads_ids[-1]
    print("\nMatchs déposés par ce chargement :")
    for r in requete_landing(f"""
        SELECT league__id, league__name, league__season, COUNT(*) AS nb_matchs
        FROM {{fx}} WHERE _dlt_load_id = '{load_id}'
        GROUP BY ALL ORDER BY league__id, league__season
    """, fx=(SOURCE, TABLE)):
        print(f"  {r['league__id']:>4} {r['league__name']:<25} {r['league__season']}  {r['nb_matchs']:>4} matchs")

    # 6.5 Quota après chargement
    apres = appeler_api(f"{BASE_URL}/status", {}, headers)["response"]["requests"]
    print(f"\n📊 {apres['current']}/{apres['limit_day']} requêtes utilisées aujourd'hui")


if __name__ == "__main__":
    main()
