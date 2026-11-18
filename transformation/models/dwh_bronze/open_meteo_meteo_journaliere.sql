-- =====================================================================
-- dwh_bronze.open_meteo_meteo_journaliere
-- Météo journalière Open-Meteo : 1 ligne par coordonnées, jour ET dépôt
-- Coordonnées demandées = latitude / longitude du géocodage (lien)
-- Renommage FR, time texte -> date, weather_code décodé, flag de version
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : la source brute, rien d'autre
-- ---------------------------------------------------------------------
WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'open_meteo_meteo_journaliere') }}
),

-- ---------------------------------------------------------------------
-- CTE 2 : renommage FR + typage
-- time, _param_start_date, _param_end_date sont du TEXTE en bronze
-- ("2022-08-12") -> essayer_cast(..., 'DATE')
-- ---------------------------------------------------------------------
renomme AS (
    SELECT
        -- ----- Clé : où et quand -----
        CAST(_param_latitude AS DOUBLE PRECISION)                 AS latitude_demandee,  -- = latitude du géocodage
        CAST(_param_longitude AS DOUBLE PRECISION)                AS longitude_demandee, -- = longitude du géocodage
        {{ essayer_cast('time', 'DATE') }}                        AS date_meteo,

        -- ----- Point de grille renvoyé par l'API -----
        CAST(grille_latitude AS DOUBLE PRECISION)                 AS latitude_grille,
        CAST(grille_longitude AS DOUBLE PRECISION)                AS longitude_grille,
        CAST(grille_altitude AS DOUBLE PRECISION)                 AS altitude_grille,
        {{ nettoyer_texte('fuseau_horaire') }}                    AS fuseau_horaire,

        -- ----- Mesures du jour (noms des variables API : _param_daily) -----
        CAST(weather_code AS INTEGER)                             AS code_meteo,
        CAST(temperature_2m_mean AS DOUBLE PRECISION)             AS temperature_moyenne_2m,
        CAST(temperature_2m_max AS DOUBLE PRECISION)              AS temperature_max_2m,
        CAST(temperature_2m_min AS DOUBLE PRECISION)              AS temperature_min_2m,
        CAST(precipitation_sum AS DOUBLE PRECISION)               AS cumul_precipitations,
        CAST(rain_sum AS DOUBLE PRECISION)                        AS cumul_pluie,
        CAST(snowfall_sum AS DOUBLE PRECISION)                    AS cumul_neige,
        CAST(wind_speed_10m_max AS DOUBLE PRECISION)              AS vitesse_vent_max_10m,

        -- ----- Paramètres de l'appel API -----
        {{ essayer_cast('_param_start_date', 'DATE') }}           AS date_debut_demandee,
        {{ essayer_cast('_param_end_date', 'DATE') }}             AS date_fin_demandee,
        {{ nettoyer_texte('_param_timezone') }}                   AS fuseau_demande,     -- "auto" dans les données

        -- ----- Colonnes techniques (traçabilité, gardées telles quelles) -----
        _date_depot,
        _fichier_source,
        _date_chargement_bronze,
        _dlt_load_id                                              AS _id_chargement_dlt
    FROM source
),

-- ---------------------------------------------------------------------
-- CTE 3 : décodage du code météo (codes WMO utilisés par Open-Meteo)
-- CASE simple : on compare code_meteo à chaque valeur
-- ---------------------------------------------------------------------
enrichi AS (
    SELECT
        *,
        CASE code_meteo
            WHEN 0  THEN 'Ciel dégagé'
            WHEN 1  THEN 'Principalement dégagé'
            WHEN 2  THEN 'Partiellement nuageux'
            WHEN 3  THEN 'Couvert'
            WHEN 45 THEN 'Brouillard'
            WHEN 48 THEN 'Brouillard givrant'
            WHEN 51 THEN 'Bruine légère'
            WHEN 53 THEN 'Bruine modérée'
            WHEN 55 THEN 'Bruine dense'
            WHEN 56 THEN 'Bruine verglaçante légère'
            WHEN 57 THEN 'Bruine verglaçante dense'
            WHEN 61 THEN 'Pluie faible'
            WHEN 63 THEN 'Pluie modérée'
            WHEN 65 THEN 'Pluie forte'
            WHEN 66 THEN 'Pluie verglaçante légère'
            WHEN 67 THEN 'Pluie verglaçante forte'
            WHEN 71 THEN 'Chute de neige faible'
            WHEN 73 THEN 'Chute de neige modérée'
            WHEN 75 THEN 'Chute de neige forte'
            WHEN 77 THEN 'Grains de neige'
            WHEN 80 THEN 'Averses de pluie faibles'
            WHEN 81 THEN 'Averses de pluie modérées'
            WHEN 82 THEN 'Averses de pluie violentes'
            WHEN 85 THEN 'Averses de neige faibles'
            WHEN 86 THEN 'Averses de neige fortes'
            WHEN 95 THEN 'Orage faible ou modéré'
            WHEN 96 THEN 'Orage avec grêle faible'
            WHEN 99 THEN 'Orage avec grêle forte'
            ELSE 'Inconnu (' || CAST(code_meteo AS VARCHAR) || ')'  -- un code imprévu apparaît au lieu d'être perdu
        END                                                       AS libelle_meteo
    FROM renomme
),

-- ---------------------------------------------------------------------
-- CTE 4 : flag de version (aucun dépôt supprimé)
-- Clé = coordonnées demandées + jour
-- ---------------------------------------------------------------------
final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY latitude_demandee, longitude_demandee, date_meteo
            ORDER BY _date_depot DESC,
                     _fichier_source DESC
        ) = 1                                                     AS est_derniere_version
    FROM enrichi
)

SELECT * FROM final