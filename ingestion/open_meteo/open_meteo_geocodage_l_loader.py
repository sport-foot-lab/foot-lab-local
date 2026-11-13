"""
=====================================================================
 open_meteo_geocodage_l_loader.py — Open-Meteo : villes -> coordonnées -> landing
=====================================================================
 Traduction locale du notebook Data Lab « open_meteo_geocodage_l_loader ».

 RÔLE
   Pour chaque ville de stade présente dans les matchs (fixture__venue__city),
   demander à l'API de géocodage Open-Meteo sa latitude / longitude
   et déposer la réponse en parquet dans :
     data/landing/open_meteo/geocodage/<aaaa>/<mm>/<jj>/<load_id>.<file_id>.parquet

 POURQUOI
   L'API météo demande des coordonnées, les matchs ne donnent qu'un nom de ville.
   -> à lancer APRÈS api_football_fixtures_l_loader.py, AVANT la météo journalière.

 NOM ENVOYÉ À L'API ET CHOIX DU BON RÉSULTAT
   - Nom NETTOYÉ (avant la 1re virgule / le « / ») ou CORRESPONDANCE manuelle (nom + pays).
   - L'API renvoie jusqu'à 10 candidats ; on en garde UN :
       1) dans le pays de la correspondance s'il est indiqué ;   « Roma » -> Italie, pas la Roumanie
       2) de préférence une VILLE (feature_code PPL…) plutôt qu'un pays, un aéroport, une montagne.
   - Chaque ligne garde la ville d'origine dans _ville_fixture : c'est elle qui sert à ne pas
     rappeler une ville, puis à relier les matchs à la météo.

 CORRIGER UNE VILLE MAL GÉOCODÉE
   Ajouter sa correspondance, puis la relancer avec --forcer : un NOUVEAU dépôt est ajouté
   (la landing garde l'historique) ; la météo utilise toujours le DERNIER dépôt de chaque ville.

 LANCEMENT (depuis la racine du dépôt, (env) activé)
   python ingestion/open_meteo/open_meteo_geocodage_l_loader.py
   python ingestion/open_meteo/open_meteo_geocodage_l_loader.py --villes "Roma; Genova" --forcer
=====================================================================
"""

# ---------------------------------------------------------------------
# PARTIE 0 — Bibliothèques et boîte à outils commune
# ---------------------------------------------------------------------
import argparse      # lire les options --villes et --forcer
import pathlib       # chemins
import sys           # pour trouver le dossier « commun »
import time          # pause entre deux appels

# On ajoute le dossier « ingestion » aux endroits où Python cherche les modules
# (même ligne que dans les loaders API-Football).
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import dlt
from commun.outils_landing import (
    appeler_api, arreter_si_rien, charger,
    pipeline_landing, requete_landing,
)


# ---------------------------------------------------------------------
# PARTIE 1 — Constantes de la source
# ---------------------------------------------------------------------
SOURCE = "open_meteo"                                         # dossier data/landing/open_meteo
TABLE = "geocodage"                                           # dossier data/landing/open_meteo/geocodage
URL = "https://geocoding-api.open-meteo.com/v1/search"        # adresse de l'API de géocodage
PAUSE_SECONDES = 1                                            # pause entre deux appels (politesse envers l'API)
NB_CANDIDATS = 10                                             # résultats demandés à l'API, parmi lesquels on choisit

# Les matchs viennent de la landing API-Football (déposés par le loader fixtures)
SOURCE_MATCHS, TABLE_MATCHS = "api_football", "fixtures"

# Villes mal trouvées avec leur nom d'origine :
#   nom de la ville dans les matchs -> (nom envoyé à l'API, code pays ISO à 2 lettres attendu)
CORRESPONDANCES = {
    "Iruñea":            ("Pamplona", "ES"),     # nom basque de Pampelune (Osasuna)
    "Levkosía":          ("Nicosia", "CY"),      # graphie grecque de Nicosie (Chypre)
    "Ta'Qali":           ("Attard", "MT"),       # stade national de Malte, sur la commune d'Attard
    "Wals-Siezenheim":   ("Salzburg", "AT"),     # stade de Salzbourg (« Wals » donnait Walsall, Angleterre)
    "Besiana, Podujevo": ("Podujevo", "XK"),     # Kosovo : nom albanais / nom serbe
    "Como":              ("Como", "IT"),         # sinon : les Comores (pays)
    "Genova":            ("Genova", "IT"),       # sinon : Génova au Guatemala
    "Roma":              ("Rome", "IT"),         # sinon : la Roumanie (pays)
    "Palma de Mallorca": ("Palma", "ES"),        # sinon : Palma de Mayorca au Mexique
    "Warszawa":          ("Warsaw", "PL"),       # sinon : un parc aux États-Unis
    "Göteborg":          ("Gothenburg", "SE"),   # sinon : une montagne du Svalbard
    "Plzeň":             ("Plzeň", "CZ"),        # sinon : l'aéroport
    "Luxembourg":        ("Luxembourg", "LU"),   # sinon : le pays (centre du Luxembourg)
    
    # --- Homonymes détectés par le contrôle du 13/11 (championnat national dans le mauvais pays) ---
    # Clé = ville telle qu'écrite dans les fixtures (_ville_fixture) ; valeur = (nom envoyé à Open-Meteo, pays ISO2 attendu)
    "Brest": ("Brest", "FR"),                     # sinon Brest en Biélorussie
    "Brentford, Middlesex": ("Brentford", "GB"),  # sinon Brentford, South Dakota (US)
    "Milano": ("Milan", "IT"),                    # sinon Milano, Texas (US) ; nom anglais comme Roma -> Rome
    "Napoli": ("Naples", "IT"),                   # sinon Napoli en Gambie ; nom anglais
}


# ---------------------------------------------------------------------
# PARTIE 2 — Les options
# ---------------------------------------------------------------------
def lire_arguments():
    p = argparse.ArgumentParser(description="Open-Meteo : géocodage des villes des stades -> landing")
    # Vide (par défaut) = toutes les villes des matchs. Rempli = seulement ces villes,
    # séparées par des POINTS-VIRGULES (certaines villes contiennent une virgule : « Besiana, Podujevo »).
    p.add_argument("--villes", default="", help="villes à géocoder, séparées par des ; (vide = toutes)")
    # --forcer : rappeler les villes même si elles sont déjà en landing (après une correction)
    p.add_argument("--forcer", action="store_true", help="rappeler aussi les villes déjà géocodées")
    return p.parse_args()


# ---------------------------------------------------------------------
# PARTIE 3 — Les villes à géocoder (lues dans la LANDING fixtures, pas dans le bronze)
# ---------------------------------------------------------------------
def villes_des_matchs() -> list[str]:
    """Toutes les villes de stade distinctes des matchs déjà déposés en landing."""
    lignes = requete_landing("""
        SELECT DISTINCT fixture__venue__city AS ville
        FROM {fx}
        WHERE fixture__venue__city IS NOT NULL
    """, fx=(SOURCE_MATCHS, TABLE_MATCHS))
    return sorted(r["ville"] for r in lignes)


def villes_deja_geocodees() -> set[str]:
    """
    Villes (nom des MATCHS) déjà présentes dans la landing géocodage : colonne _ville_fixture.
    1er lancement : pas encore de fichier -> ensemble vide.
    """
    lignes = requete_landing("SELECT DISTINCT _ville_fixture FROM {geo}", geo=(SOURCE, TABLE))
    return {r["_ville_fixture"] for r in lignes}


def nom_a_chercher(ville: str) -> tuple[str, str | None]:
    """
    (nom envoyé à l'API, pays attendu) pour une ville des matchs :
      1) correspondance manuelle si elle existe ;  « Roma » -> (« Roma », « IT »)
      2) sinon le texte avant la 1re virgule et avant « / », sans pays imposé.
         « Leeds, West Yorkshire » -> (« Leeds », None)
    """
    if ville in CORRESPONDANCES:
        return CORRESPONDANCES[ville]
    return ville.split(",")[0].split(" / ")[0].strip(), None


# ---------------------------------------------------------------------
# PARTIE 4 — Lire la réponse de l'API et choisir LE bon résultat
# ---------------------------------------------------------------------
def verifier_erreurs(reponse: dict, params: dict) -> None:
    """Open-Meteo signale une erreur par "error": true + un message dans "reason"."""
    if reponse.get("error"):
        raise RuntimeError(f"Erreur Open-Meteo ({params}) : {reponse.get('reason')}")


def choisir_ligne(reponse: dict, pays: str | None) -> list[dict]:
    """
    Les candidats sont dans "results" (clé ABSENTE si rien trouvé). On en garde un seul :
      1) si un pays est attendu : seulement les candidats de ce pays (country_code) ;
      2) parmi eux, la 1re VILLE (feature_code PPL…), sinon le 1er candidat.
    Renvoie [ligne] ou [] (non trouvée).
    postcodes est une LISTE : on la transforme en texte « 69150, 69151 » (pas de table enfant).
    """
    candidats = reponse.get("results", [])
    if pays:
        candidats = [c for c in candidats if c.get("country_code") == pays]
    villes = [c for c in candidats if str(c.get("feature_code", "")).startswith("PPL")]
    choix = (villes or candidats)[:1]
    for ligne in choix:
        if "postcodes" in ligne:
            ligne["postcodes"] = ", ".join(ligne["postcodes"])
    return choix


# ---------------------------------------------------------------------
# PARTIE 5 — La ressource dlthub
# ---------------------------------------------------------------------
def ressource_geocodage(villes: list[str], trouvees: list[str]):
    """trouvees : liste remplie au fur et à mesure (sert au bilan de fin)."""
    @dlt.resource(name=TABLE, write_disposition="append")    # append : la landing garde tous les dépôts
    def geocodage():
        for i, ville in enumerate(villes):
            if i > 0:
                time.sleep(PAUSE_SECONDES)
            nom, pays = nom_a_chercher(ville)
            params = {"name": nom, "count": NB_CANDIDATS}
            reponse = appeler_api(URL, params)                # pas de headers : API sans clé
            verifier_erreurs(reponse, params)
            lignes = choisir_ligne(reponse, pays)
            envoye = "" if nom == ville and not pays else f" -> « {nom} »" + (f" [{pays}]" if pays else "")
            if lignes:
                trouvees.append(ville)
                print(f"📥 {ville}{envoye} : {lignes[0].get('name')} ({lignes[0].get('country')})")
            else:
                print(f"📥 {ville}{envoye} : ⚠️ ville non trouvée")
            # On ajoute : la ville d'origine (_ville_fixture) + les paramètres de l'appel (_param_name, _param_count)
            yield [{**ligne, "_ville_fixture": ville, **{f"_param_{k}": v for k, v in params.items()}}
                   for ligne in lignes]
    return geocodage


# ---------------------------------------------------------------------
# PARTIE 6 — Programme principal
# ---------------------------------------------------------------------
def main():
    args = lire_arguments()

    # 6.1 Villes demandées : celles tapées (séparées par ;) ou toutes celles des matchs
    villes = [v.strip() for v in args.villes.split(";") if v.strip()] or villes_des_matchs()
    arreter_si_rien(villes, "aucune ville dans la landing fixtures (lancer d'abord le loader fixtures).")

    # 6.2 Moins celles déjà géocodées (sauf avec --forcer)
    deja = set() if args.forcer else villes_deja_geocodees()
    a_appeler = [v for v in villes if v not in deja]
    print(f"Villes demandées : {len(villes)} | déjà en landing : {len(deja & set(villes))} "
          f"| à appeler : {len(a_appeler)}" + (" (--forcer)" if args.forcer else ""))
    arreter_si_rien(a_appeler, "toutes les villes demandées sont déjà géocodées en landing.")

    # 6.3 Chargement API -> landing (parquet)
    trouvees = []
    pipeline = pipeline_landing(SOURCE, TABLE)
    charger(pipeline, ressource_geocodage(a_appeler, trouvees))

    # 6.4 Bilan de CE lancement
    non_trouvees = [v for v in a_appeler if v not in trouvees]
    print(f"\n✅ {len(trouvees)} ville(s) géocodée(s) par ce lancement")
    if non_trouvees:
        print(f"⚠️ {len(non_trouvees)} ville(s) non trouvée(s) par l'API (retentées au prochain lancement) :")
        for v in non_trouvees:
            print(f"   - {v}  (envoyé : {nom_a_chercher(v)}) -> corriger dans CORRESPONDANCES")


if __name__ == "__main__":
    main()