-- =====================================================================
-- dwh_silver.dwh_silver_dim_ligue
-- Dimension LIGUE : 1 ligne par ligue (id_ligue = clé)
-- Partagée par tous les faits (projet 4 stats joueurs, projet 2 matchs...)
-- Sources : les 4 listes top* ET les matchs (fixtures) de dwh_bronze
-- Règle (choix E3) : si une ligue a plusieurs noms/logos,
--                    on prend celui des top* en priorité, sinon celui des matchs
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : les ligues vues dans les 4 listes top*
-- UNION (sans ALL) = supprime les doublons : chaque ligue apparaît
-- 20 fois par saison dans chaque liste, on n'en garde qu'une ligne
-- 'top' = étiquette de la source, utilisée pour la priorité (CTE 4)
-- ---------------------------------------------------------------------
WITH ligues_top AS (
    SELECT id_ligue, nom_ligue, url_logo_ligue, 'top' AS source
    FROM {{ ref('api_football_topscorers') }}
    UNION
    SELECT id_ligue, nom_ligue, url_logo_ligue, 'top' AS source
    FROM {{ ref('api_football_topassists') }}
    UNION
    SELECT id_ligue, nom_ligue, url_logo_ligue, 'top' AS source
    FROM {{ ref('api_football_topyellowcards') }}
    UNION
    SELECT id_ligue, nom_ligue, url_logo_ligue, 'top' AS source
    FROM {{ ref('api_football_topredcards') }}
),

-- ---------------------------------------------------------------------
-- CTE 2 : les ligues vues dans les matchs
-- DISTINCT : 1 ligne par combinaison id / nom / logo (au lieu de 6 048)
-- ---------------------------------------------------------------------
ligues_matchs AS (
    SELECT DISTINCT id_ligue, nom_ligue, url_logo_ligue, 'matchs' AS source
    FROM {{ ref('api_football_fixtures') }}
),

-- ---------------------------------------------------------------------
-- CTE 3 : toutes les ligues des deux sources, l'une sous l'autre
-- UNION ALL : on garde tout, la CTE 4 choisira UNE ligne par ligue
-- ---------------------------------------------------------------------
toutes_les_ligues AS (
    SELECT * FROM ligues_top
    UNION ALL
    SELECT * FROM ligues_matchs
),

-- ---------------------------------------------------------------------
-- CTE 4 : numéroter les lignes de chaque ligue selon la priorité
--   PARTITION BY id_ligue : on recommence la numérotation pour chaque ligue
--   ORDER BY priorité     : CASE = top* d'abord (1), matchs ensuite (2)
--   puis nom_ligue        : départage stable si une source a 2 noms
-- La ligne gagnante de chaque ligue reçoit rang = 1
-- ---------------------------------------------------------------------
classement AS (
    SELECT
        id_ligue,
        nom_ligue,
        url_logo_ligue,
        ROW_NUMBER() OVER (
            PARTITION BY id_ligue
            ORDER BY
                CASE source
                    WHEN 'top'    THEN 1
                    WHEN 'matchs' THEN 2
                END,
                nom_ligue
        ) AS rang
    FROM toutes_les_ligues
)

-- ---------------------------------------------------------------------
-- Résultat : 1 ligne par ligue (la gagnante), colonnes validées
-- ---------------------------------------------------------------------
SELECT
    id_ligue,          -- clé de la dimension (reliée à id_ligue des faits)
    nom_ligue,
    url_logo_ligue     -- URL de l'image (Power BI : catégorie « URL d'image »)
FROM classement
WHERE rang = 1