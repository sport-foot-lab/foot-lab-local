-- =====================================================================
-- dwh_bronze.api_football_fixtures
-- 1 ligne par match ET par dépôt (tous les dépôts sont gardés)
-- Renommage FR, typage, nettoyage, décodage, flag est_derniere_version
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : la source brute, rien d'autre
-- source('bronze', ...) = la table déclarée dans _sources.yml
-- ---------------------------------------------------------------------
WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'api_football_fixtures') }}
),

-- ---------------------------------------------------------------------
-- CTE 2 : renommage FR + typage explicite (CAST) + nettoyage des textes
-- Les URL (logos) et les colonnes techniques ne sont PAS nettoyées :
-- ce sont des identifiants, ils restent tels quels.
-- ---------------------------------------------------------------------
renomme AS (
    SELECT
        -- ----- Match -----
        CAST(fixture__id AS BIGINT)                               AS id_match,
        -- timestamptz -> heure UTC sans fuseau (même résultat en dev et en prod)
        fixture__date AT TIME ZONE 'UTC'                          AS date_heure_match_utc,
        CAST(fixture__date AT TIME ZONE 'UTC' AS DATE)            AS date_match,
        -- nombre de secondes depuis 1970 -> date-heure, puis UTC
        TO_TIMESTAMP(fixture__periods__first)  AT TIME ZONE 'UTC' AS debut_1re_periode_utc,
        TO_TIMESTAMP(fixture__periods__second) AT TIME ZONE 'UTC' AS debut_2e_periode_utc,
        {{ nettoyer_texte('fixture__status__short') }}            AS code_statut,
        CAST(fixture__status__elapsed AS INTEGER)                 AS minutes_ecoulees,
        CAST(fixture__status__extra AS INTEGER)                   AS statut_extra,   -- valeur brute API status.extra

        -- ----- Ligue / saison / tour -----
        CAST(league__id AS BIGINT)                                AS id_ligue,
        {{ nettoyer_texte('league__name') }}                      AS nom_ligue,
        {{ nettoyer_texte('league__country') }}                   AS pays_ligue,
        CAST(league__season AS INTEGER)                           AS saison,
        {{ nettoyer_texte('league__round') }}                     AS libelle_tour,
        league__standings                                         AS ligue_avec_classement,
        league__logo                                              AS url_logo_ligue,
        league__flag                                              AS url_drapeau_ligue,

        -- ----- Arbitre / stade -----
        {{ nettoyer_texte('fixture__referee') }}                  AS arbitre_complet,
        CAST(fixture__venue__id AS BIGINT)                        AS id_stade,
        {{ nettoyer_texte('fixture__venue__name') }}              AS nom_stade,
        {{ nettoyer_texte('fixture__venue__city') }}              AS ville_stade_complete,

        -- ----- Équipes -----
        CAST(teams__home__id AS BIGINT)                           AS id_equipe_domicile,
        {{ nettoyer_texte('teams__home__name') }}                 AS nom_equipe_domicile,
        teams__home__logo                                         AS url_logo_equipe_domicile,
        teams__home__winner                                       AS domicile_gagnant,
        CAST(teams__away__id AS BIGINT)                           AS id_equipe_exterieur,
        {{ nettoyer_texte('teams__away__name') }}                 AS nom_equipe_exterieur,
        teams__away__logo                                         AS url_logo_equipe_exterieur,
        teams__away__winner                                       AS exterieur_gagnant,

        -- ----- Scores -----
        -- buts = temps réglementaire + prolongation (vérifié sur les 6 048 lignes)
        CAST(goals__home AS INTEGER)                              AS buts_domicile,
        CAST(goals__away AS INTEGER)                              AS buts_exterieur,
        CAST(score__halftime__home AS INTEGER)                    AS score_mi_temps_domicile,
        CAST(score__halftime__away AS INTEGER)                    AS score_mi_temps_exterieur,
        CAST(score__fulltime__home AS INTEGER)                    AS score_temps_reglementaire_domicile,
        CAST(score__fulltime__away AS INTEGER)                    AS score_temps_reglementaire_exterieur,
        CAST(score__extratime__home AS INTEGER)                   AS score_prolongation_domicile,
        CAST(score__extratime__away AS INTEGER)                   AS score_prolongation_exterieur,
        CAST(score__penalty__home AS INTEGER)                     AS score_tab_domicile,      -- tab = tirs au but
        CAST(score__penalty__away AS INTEGER)                     AS score_tab_exterieur,

        -- ----- Colonnes techniques (traçabilité, gardées telles quelles) -----
        _date_depot,
        _fichier_source,
        _date_chargement_bronze,
        _dlt_load_id                                              AS _id_chargement_dlt
    FROM source
),


-- ---------------------------------------------------------------------
-- CTE 3 : colonnes dérivées (découpage + décodage)
-- ---------------------------------------------------------------------
enrichi AS (
    SELECT
        *,

        -- ----- Tour : "Regular Season - 12" -> phase + numéro de journée -----
        -- morceau avant " - " (si pas de " - ", le texte entier : "Final", "Round of 16")
        TRIM(SPLIT_PART(libelle_tour, ' - ', 1))                  AS phase,
        -- numéro SEULEMENT si le tour finit par " - <chiffres>"
        -- ("Round of 16" ne correspond pas : 16 n'est pas une journée)
        CASE
            WHEN libelle_tour ~ '^.* - [0-9]+$'
            THEN CAST(REGEXP_REPLACE(libelle_tour, '^.* - ', '') AS INTEGER)
        END                                                       AS numero_journee,

        -- ----- Arbitre : "Nom, Pays" -> nom / pays -----
        -- pays = après la DERNIÈRE virgule ("Brooks, John, England" -> "England")
        CASE
            WHEN arbitre_complet LIKE '%,%'
            THEN TRIM(REGEXP_REPLACE(arbitre_complet, ',[^,]*$', ''))   -- retire ", Pays"
            ELSE arbitre_complet                                        -- "R. Jones" : pas de pays
        END                                                       AS nom_arbitre,
        CASE
            WHEN arbitre_complet LIKE '%,%'
            THEN TRIM(REGEXP_REPLACE(arbitre_complet, '^.*,', ''))      -- retire "Nom, "
        END                                                       AS pays_arbitre,

        -- ----- Ville : "Leeds, West Yorkshire" -> ville / région -----
        TRIM(SPLIT_PART(ville_stade_complete, ',', 1))            AS ville_stade,
        NULLIF(TRIM(REGEXP_REPLACE(ville_stade_complete, '^[^,]*,?', '')), '') AS region_stade,

        -- ----- Statut décodé -----
        -- Vérifié dans les données : les 19 AET ont un score de prolongation,
        -- les 17 PEN ont un score de tirs au but
        CASE code_statut
            WHEN 'FT'  THEN 'Terminé'
            WHEN 'AET' THEN 'Terminé après prolongation'
            WHEN 'PEN' THEN 'Terminé aux tirs au but'
            ELSE 'Inconnu (' || code_statut || ')'     -- un futur code apparaîtra au lieu d'être perdu
        END                                                       AS libelle_statut,

        -- ----- Résultat (d'après les indicateurs "winner" de l'API) -----
        CASE
            WHEN domicile_gagnant  THEN 'Victoire domicile'
            WHEN exterieur_gagnant THEN 'Victoire extérieur'
            ELSE 'Nul'
        END                                                       AS resultat
    FROM renomme
),




-- ---------------------------------------------------------------------
-- CTE 4 : flag de version (aucun dépôt supprimé)
-- ---------------------------------------------------------------------
final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_match                       -- un groupe par match
            ORDER BY _date_depot DESC,                  -- le plus récent d'abord
                     _fichier_source DESC               -- départage si même date
        ) = 1                                                     AS est_derniere_version
    FROM enrichi
)

SELECT * FROM final