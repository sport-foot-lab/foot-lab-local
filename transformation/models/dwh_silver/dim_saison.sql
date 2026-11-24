-- =====================================================================
-- dwh_silver.dim_saison
-- Dimension SAISON : 1 ligne par saison (saison = clé)
-- Partagée par tous les faits : UN seul segment « saison » dans Power BI
-- filtrera à la fois les stats joueurs (projet 4) et les matchs (projet 2)
-- Sources : les 4 listes top* ET les matchs (fixtures) de dwh_bronze
-- Aucune année écrite en dur : les saisons viennent des données,
-- une nouvelle saison chargée (2025, 2026...) apparaît toute seule
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : toutes les saisons rencontrées, dans les 5 tables
-- UNION (sans ALL) = supprime les doublons : chaque saison
-- est présente des centaines de fois, on n'en garde qu'une ligne
-- ---------------------------------------------------------------------
WITH saisons AS (
    SELECT saison FROM {{ ref('api_football_topscorers') }}
    UNION
    SELECT saison FROM {{ ref('api_football_topassists') }}
    UNION
    SELECT saison FROM {{ ref('api_football_topyellowcards') }}
    UNION
    SELECT saison FROM {{ ref('api_football_topredcards') }}
    UNION
    SELECT saison FROM {{ ref('api_football_fixtures') }}
)

-- ---------------------------------------------------------------------
-- Résultat : la saison + son libellé (choix S1 : concaténation)
--   CAST(... AS VARCHAR) : on transforme le nombre en texte
--                          avant de le coller (même SQL en DuckDB et Postgres)
--   ||                   : colle les morceaux de texte
--   saison + 1           : l'année de fin (une saison 2022 va de 2022 à 2023)
--   2022 -> '2022-2023'
-- ---------------------------------------------------------------------
SELECT
    saison,                                            -- clé de la dimension (reliée à saison des faits)
    CAST(saison AS VARCHAR) || '-'
        || CAST(saison + 1 AS VARCHAR)    AS libelle_saison
FROM saisons
WHERE saison IS NOT NULL                               -- sécurité : pas de clé vide dans une dimension