-- =====================================================================
-- dwh_bronze.open_meteo_geocodage
-- Géocodage Open-Meteo des villes de stade : 1 ligne par ville
-- = la version EN COURS du snapshot (dbt_valid_to IS NULL)
-- L'historique des dépôts est dans dwh_snapshot.open_meteo_geocodage_snapshot
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : la version en cours de chaque ville, lue dans le snapshot
-- ---------------------------------------------------------------------
WITH source AS (
    SELECT *
    FROM {{ ref('open_meteo_geocodage_snapshot') }}   -- historique SCD2 (colonnes brutes)
    WHERE dbt_valid_to IS NULL                         -- seulement la version en cours
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
)

-- Plus de flag est_derniere_version : le filtre dbt_valid_to IS NULL (CTE 1) l'a remplacé
SELECT * FROM renomme