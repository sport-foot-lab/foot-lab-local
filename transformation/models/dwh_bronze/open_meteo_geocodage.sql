-- =====================================================================
-- dwh_bronze.open_meteo_geocodage
-- Géocodage Open-Meteo des villes de stade : 1 ligne par ville ET par dépôt
-- Un dépôt plus récent peut CORRIGER une ville mal géocodée
-- (ex. Brest : 1er dépôt en Biélorussie (BY), dépôt suivant en France (FR))
-- -> est_derniere_version = le géocodage à utiliser
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : la source brute, rien d'autre
-- ---------------------------------------------------------------------
WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'open_meteo_geocodage') }}
),

-- ---------------------------------------------------------------------
-- CTE 2 : renommage FR + typage + nettoyage des textes
-- ---------------------------------------------------------------------
renomme AS (
    SELECT
        -- ----- Lien avec les matchs -----
        -- même valeur que ville_stade_complete de dwh_bronze.api_football_fixtures
        -- (les 209 villes des matchs sont toutes présentes ici)
        {{ nettoyer_texte('_ville_fixture') }}                    AS ville_stade_complete,
        {{ nettoyer_texte('_param_name') }}                       AS nom_recherche,      -- nom envoyé à l'API (paramètre name)
        CAST(_param_count AS INTEGER)                             AS nb_resultats_demandes, -- paramètre count de l'appel

        -- ----- Lieu trouvé -----
        CAST(id AS BIGINT)                                        AS id_lieu,
        {{ nettoyer_texte('name') }}                              AS nom_lieu,
        CAST(latitude AS DOUBLE PRECISION)                        AS latitude,
        CAST(longitude AS DOUBLE PRECISION)                       AS longitude,
        CAST(elevation AS DOUBLE PRECISION)                       AS altitude,
        {{ nettoyer_texte('feature_code') }}                      AS code_type_lieu,     -- feature_code (codes GeoNames : PPLC, PPLA…)
        {{ nettoyer_texte('timezone') }}                          AS fuseau_horaire,
        CAST(population AS BIGINT)                                AS population,
        {{ nettoyer_texte('postcodes') }}                         AS codes_postaux,      -- liste en texte, séparée par des virgules

        -- ----- Pays -----
        CAST(country_id AS BIGINT)                                AS id_pays,
        {{ nettoyer_texte('country_code') }}                      AS code_pays,
        {{ nettoyer_texte('country') }}                           AS pays,

        -- ----- Découpage administratif (1 = 1er niveau, 2 = 2e niveau…) -----
        CAST(admin1_id AS BIGINT)                                 AS id_admin1,
        {{ nettoyer_texte('admin1') }}                            AS nom_admin1,
        CAST(admin2_id AS BIGINT)                                 AS id_admin2,
        {{ nettoyer_texte('admin2') }}                            AS nom_admin2,
        CAST(admin3_id AS BIGINT)                                 AS id_admin3,
        {{ nettoyer_texte('admin3') }}                            AS nom_admin3,
        CAST(admin4_id AS BIGINT)                                 AS id_admin4,
        {{ nettoyer_texte('admin4') }}                            AS nom_admin4,

        -- ----- Colonnes techniques (traçabilité, gardées telles quelles) -----
        _date_depot,
        _fichier_source,
        _date_chargement_bronze,
        _dlt_load_id                                              AS _id_chargement_dlt
    FROM source
),

-- ---------------------------------------------------------------------
-- CTE 3 : flag de version (aucun dépôt supprimé)
-- Clé = la ville du match : 1 paquet par ville, le dépôt le plus récent gagne
-- ---------------------------------------------------------------------
final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ville_stade_complete
            ORDER BY _date_depot DESC,
                     _fichier_source DESC
        ) = 1                                                     AS est_derniere_version
    FROM renomme
)

SELECT * FROM final


