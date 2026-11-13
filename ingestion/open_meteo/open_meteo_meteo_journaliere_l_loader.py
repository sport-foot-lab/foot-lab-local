"""
=====================================================================
 open_meteo_meteo_journaliere_l_loader.py — Open-Meteo : météo des jours de match -> landing
=====================================================================
 Traduction locale du notebook Data Lab « open_meteo_meteo_journaliere_l_loader ».

 RÔLE
   Pour chaque couple (coordonnées du stade, jour de match), demander à l'API
   d'archives Open-Meteo la météo de la journée et la déposer en parquet dans :
     data/landing/open_meteo/meteo_journaliere/<aaaa>/<mm>/<jj>/<load_id>.<file_id>.parquet

 D'OÙ VIENNENT LES COUPLES (tout est lu dans la LANDING, pas dans le bronze)
   matchs (api_football/fixtures)  JOIN  géocodage (open_meteo/geocodage)
     ON ville du stade = _ville_fixture
   -> à lancer APRÈS fixtures et APRÈS géocodage.

 ÉCONOMIE D'APPELS (gratuit : 600 / minute, 5 000 / heure, 10 000 / jour)
   - 1 appel = 1 jour, 8 variables (sous le seuil de 10 qui ferait compter l'appel double) ;
   - un couple déjà en landing n'est PAS rappelé (la météo passée ne change pas) ;
   - seulement les jours d'il y a 6 jours ou plus (l'archive met ~5 jours à se remplir) ;
   - au plus --lot-max appels par lancement (2 000 par défaut) : le reste au lancement suivant.

 LANCEMENT (depuis la racine du dépôt, (env) activé)
   python ingestion/open_meteo/open_meteo_meteo_journaliere_l_loader.py
   python ingestion/open_meteo/open_meteo_meteo_journaliere_l_loader.py --villes "Nantes" --lot-max 5
=====================================================================
"""

# ---------------------------------------------------------------------
# PARTIE 0 — Bibliothèques et boîte à outils commune
# ---------------------------------------------------------------------
import argparse      # lire les options --villes et --lot-max (= les widgets du notebook)
import pathlib       # chemins
import sys           # pour trouver le dossier « commun » + arrêt en erreur
import time          # pause entre deux appels

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))   # dossier « ingestion »

import dlt
from commun.outils_landing import (
    appeler_api, arreter_si_rien, charger, liste_textes,
    pipeline_landing, requete_landing,
)


# ---------------------------------------------------------------------
# PARTIE 1 — Constantes de la source (identiques au notebook)
# ---------------------------------------------------------------------
SOURCE = "open_meteo"                                     # dossier data/landing/open_meteo
TABLE = "meteo_journaliere"                               # dossier data/landing/open_meteo/meteo_journaliere
URL = "https://archive-api.open-meteo.com/v1/archive"     # API des archives météo (sans clé)
PAUSE_SECONDES = 0.2                                      # 600 appels / minute max -> 0,2 s laisse de la marge
LIMITE_JOUR = 9000                                        # garde-fou : limite gratuite = 10 000 appels / jour

# Les 8 variables journalières demandées (code météo, températures, pluie, neige, vent)
VARIABLES_JOUR = ("weather_code,temperature_2m_mean,temperature_2m_max,temperature_2m_min,"
                  "precipitation_sum,rain_sum,snowfall_sum,wind_speed_10m_max")

# Type imposé à chaque variable : si une variable est vide (null) dans tout un lot,
# dlthub ne créerait pas sa colonne (même avertissement que pour les top* d'API-Football).
TYPES_IMPOSES = {
    "weather_code":        {"data_type": "bigint", "nullable": True},   # code météo WMO (0 = ciel clair…)
    "temperature_2m_mean": {"data_type": "double", "nullable": True},   # °C
    "temperature_2m_max":  {"data_type": "double", "nullable": True},
    "temperature_2m_min":  {"data_type": "double", "nullable": True},
    "precipitation_sum":   {"data_type": "double", "nullable": True},   # mm
    "rain_sum":            {"data_type": "double", "nullable": True},
    "snowfall_sum":        {"data_type": "double", "nullable": True},   # cm
    "wind_speed_10m_max":  {"data_type": "double", "nullable": True},   # km/h
}

# Tables de la landing utilisées pour construire la liste des appels
MATCHS = ("api_football", "fixtures")
GEOCODAGE = ("open_meteo", "geocodage")


# ---------------------------------------------------------------------
# PARTIE 2 — Les options (= les widgets VILLES et LOT_MAX du notebook)
# ---------------------------------------------------------------------
def lire_arguments():
    p = argparse.ArgumentParser(description="Open-Meteo : météo des jours de match -> landing")
    p.add_argument("--villes", default="", help="villes des matchs à traiter (vide = toutes) — mode test")
    p.add_argument("--lot-max", type=int, default=2000, help="nombre max d'appels par lancement")
    return p.parse_args()


# ---------------------------------------------------------------------
# PARTIE 3 — Les couples (latitude, longitude, jour) à demander
# ---------------------------------------------------------------------
def couples_matchs(villes: list[str]) -> list[tuple[float, float, str]]:
    """
    Un couple par stade × jour de match, d'après la landing :
      - fixtures donne la ville du stade et la date du match ;
      - geocodage donne les coordonnées de cette ville (_ville_fixture = nom de la ville dans les matchs).
    timezone('UTC', …) : la date du match est prise en heure UTC (comme dans les données de l'API).
    """
    filtre = ""
    if villes:                                            # mode test : seulement ces villes
        liste = ", ".join("'" + v.replace("'", "''") + "'" for v in villes)   # ' doublé : Ta'Qali
        filtre = f"AND g._ville_fixture IN ({liste})"
    lignes = requete_landing(f"""
        SELECT DISTINCT
               CAST(g.latitude AS DOUBLE)  AS latitude,                        -- DOUBLE : comparaison exacte
               CAST(g.longitude AS DOUBLE) AS longitude,                       -- avec _param_latitude/longitude
               CAST(CAST(timezone('UTC', CAST(f.fixture__date AS TIMESTAMPTZ)) AS DATE) AS VARCHAR) AS jour
        FROM {{fx}} f
        JOIN (                                                                 -- géocodage : DERNIER dépôt de chaque ville
            SELECT * FROM {{geo}}                                              -- (une ville corrigée a un dépôt plus récent)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY _ville_fixture
                                       ORDER BY CAST(_dlt_load_id AS DOUBLE) DESC) = 1
        ) g
        ON f.fixture__venue__city = g._ville_fixture                         -- ville du stade -> coordonnées                       -- ville du stade -> coordonnées
        WHERE CAST(timezone('UTC', CAST(f.fixture__date AS TIMESTAMPTZ)) AS DATE)
              <= current_date - INTERVAL 6 DAY                                 -- archive dispo après ~5 jours
        {filtre}
        ORDER BY jour, latitude, longitude
    """, fx=MATCHS, geo=GEOCODAGE)
    return [(r["latitude"], r["longitude"], r["jour"]) for r in lignes]


def couples_deja_charges() -> set[tuple[float, float, str]]:
    """Couples déjà en landing météo : les colonnes _param_ = paramètres exacts de l'appel."""
    lignes = requete_landing("""
        SELECT DISTINCT CAST(_param_latitude AS DOUBLE) AS lat, CAST(_param_longitude AS DOUBLE) AS lon,
               CAST(_param_start_date AS VARCHAR) AS jour
        FROM {meteo}
    """, meteo=(SOURCE, TABLE))
    return {(r["lat"], r["lon"], r["jour"]) for r in lignes}


# ---------------------------------------------------------------------
# PARTIE 4 — Lire la réponse de l'API
# ---------------------------------------------------------------------
def verifier_erreurs(reponse: dict, params: dict) -> None:
    """Open-Meteo signale une erreur par "error": true + un message dans "reason"."""
    if reponse.get("error"):
        raise RuntimeError(f"Erreur Open-Meteo ({params}) : {reponse.get('reason')}")


def extraire_lignes(reponse: dict) -> list[dict]:
    """
    "daily" arrive en COLONNES : {"time": ["2024-03-02"], "rain_sum": [1.2], ...}
    zip(...) les remet en LIGNES : [{"time": "2024-03-02", "rain_sum": 1.2, ...}] (1 ligne par jour).
    On ajoute le point de grille météo réellement utilisé par Open-Meteo (utile plus tard en silver).
    """
    jour = reponse.get("daily", {})
    lignes = [dict(zip(jour.keys(), valeurs)) for valeurs in zip(*jour.values())]
    for ligne in lignes:
        ligne["grille_latitude"] = reponse.get("latitude")
        ligne["grille_longitude"] = reponse.get("longitude")
        ligne["grille_altitude"] = reponse.get("elevation")
        ligne["fuseau_horaire"] = reponse.get("timezone")
    return lignes


# ---------------------------------------------------------------------
# PARTIE 5 — La ressource dlthub
# ---------------------------------------------------------------------
def ressource_meteo(appels: list[dict]):
    @dlt.resource(name=TABLE, write_disposition="append", columns=TYPES_IMPOSES)   # append + types imposés
    def meteo_journaliere():
        for i, params in enumerate(appels):
            if i > 0:
                time.sleep(PAUSE_SECONDES)
            reponse = appeler_api(URL, params)                # pas de headers : API sans clé
            verifier_erreurs(reponse, params)
            lignes = extraire_lignes(reponse)
            if (i + 1) % 100 == 0 or i + 1 == len(appels):    # un message tous les 100 appels (sinon 2 000 lignes à l'écran)
                print(f"📥 {i + 1}/{len(appels)} appels faits (dernier : {params['start_date']}, "
                      f"{params['latitude']}, {params['longitude']})")
            # On ajoute les paramètres de l'appel : _param_latitude, _param_longitude, _param_start_date…
            yield [{**ligne, **{f"_param_{k}": v for k, v in params.items()}} for ligne in lignes]
    return meteo_journaliere


# ---------------------------------------------------------------------
# PARTIE 6 — Programme principal
# ---------------------------------------------------------------------
def main():
    args = lire_arguments()
    if args.lot_max > LIMITE_JOUR:                            # garde-fou du notebook
        sys.exit(f"❌ --lot-max {args.lot_max} : au-dessus de la limite journalière Open-Meteo ({LIMITE_JOUR}). Arrêt.")

    # 6.1 Couples demandés (matchs × géocodage), moins ceux déjà en landing
    demandes = couples_matchs(liste_textes(args.villes))
    arreter_si_rien(demandes, "aucun couple stade × jour (lancer d'abord fixtures puis géocodage).")
    deja = couples_deja_charges()
    restants = [c for c in demandes if c not in deja]
    print(f"Couples stade × jour : {len(demandes)} | déjà en landing : {len(demandes) - len(restants)} "
          f"| restants : {len(restants)}")
    arreter_si_rien(restants, "tous les jours de match sont déjà en landing.")

    # 6.2 On découpe : au plus --lot-max appels maintenant, le reste aux lancements suivants
    if len(restants) > args.lot_max:
        print(f"✂️ Lot limité à {args.lot_max} appels ; {len(restants) - args.lot_max} pour les lancements suivants")
        restants = restants[:args.lot_max]

    # 6.3 Un appel = un couple, sur UN seul jour (start_date = end_date)
    appels = [{"latitude": lat, "longitude": lon, "start_date": jour, "end_date": jour,
               "daily": VARIABLES_JOUR, "timezone": "auto"}           # auto : journée à l'heure locale du stade
              for (lat, lon, jour) in restants]
    print(f"{len(appels)} appel(s) à faire (environ {len(appels) * 0.5 / 60:.0f} min)")

    # 6.4 Chargement API -> landing (parquet)
    pipeline = pipeline_landing(SOURCE, TABLE)
    charger(pipeline, ressource_meteo(appels))

    # 6.5 Bilan : couples encore manquants après ce lancement
    manquants = len([c for c in demandes if c not in couples_deja_charges()])
    print(f"\n✅ Lot terminé | couples encore à charger : {manquants}"
          + ("" if manquants == 0 else " -> relancer le script (ou laisser le DAG le faire)"))


if __name__ == "__main__":
    main()