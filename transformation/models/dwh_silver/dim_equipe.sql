-- =====================================================================
-- dwh_silver.dim_equipe
-- Dimension ÉQUIPE : 1 ligne par équipe (id_equipe = clé)
-- Partagée par tous les faits (projet 4 stats joueurs, projet 2 matchs...)
-- Sources : les 4 listes top* ET les matchs (fixtures) de dwh_bronze
--           (fixtures : équipe à domicile ET équipe à l'extérieur)
-- Règle du nom unique (choix E3 + départage validé) :
--   1. le nom des top* en priorité
--   2. sinon le nom des matchs
--   3. à priorité égale : le nom du match le plus récent
--   ex. id 157 : « Bayern Munich » / « Bayern München » -> « Bayern München »
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : les équipes vues dans les 4 listes top*
-- UNION (sans ALL) = supprime les doublons
-- date_match : n'existe pas dans les top* -> NULL, typé DATE
--              (pour pouvoir empiler avec les matchs en CTE 3)
-- ---------------------------------------------------------------------
WITH equipes_top AS (
    SELECT id_equipe, nom_equipe, url_logo_equipe,
           'top' AS source, CAST(NULL AS DATE) AS date_match
    FROM {{ ref('api_football_topscorers') }}
    UNION
    SELECT id_equipe, nom_equipe, url_logo_equipe,
           'top' AS source, CAST(NULL AS DATE) AS date_match
    FROM {{ ref('api_football_topassists') }}
    UNION
    SELECT id_equipe, nom_equipe, url_logo_equipe,
           'top' AS source, CAST(NULL AS DATE) AS date_match
    FROM {{ ref('api_football_topyellowcards') }}
    UNION
    SELECT id_equipe, nom_equipe, url_logo_equipe,
           'top' AS source, CAST(NULL AS DATE) AS date_match
    FROM {{ ref('api_football_topredcards') }}
),

-- ---------------------------------------------------------------------
-- CTE 2 : les équipes vues dans les matchs
-- Un match a 2 équipes : on prend les colonnes « domicile » PUIS
-- les colonnes « extérieur », renommées pareil, l'une sous l'autre
-- On garde date_match pour le départage « match le plus récent »
-- ---------------------------------------------------------------------
equipes_matchs AS (
    SELECT id_equipe_domicile        AS id_equipe,
           nom_equipe_domicile       AS nom_equipe,
           url_logo_equipe_domicile  AS url_logo_equipe,
           'matchs'                  AS source,
           date_match
    FROM {{ ref('api_football_fixtures') }}
    UNION ALL
    SELECT id_equipe_exterieur       AS id_equipe,
           nom_equipe_exterieur      AS nom_equipe,
           url_logo_equipe_exterieur AS url_logo_equipe,
           'matchs'                  AS source,
           date_match
    FROM {{ ref('api_football_fixtures') }}
),

-- ---------------------------------------------------------------------
-- CTE 3 : toutes les équipes des deux sources, l'une sous l'autre
-- ---------------------------------------------------------------------
toutes_les_equipes AS (
    SELECT * FROM equipes_top
    UNION ALL
    SELECT * FROM equipes_matchs
),

-- ---------------------------------------------------------------------
-- CTE 4 : numéroter les lignes de chaque équipe selon la règle
--   PARTITION BY id_equipe        : numérotation recommencée par équipe
--   1. CASE source                : top* d'abord (1), matchs ensuite (2)
--   2. date_match DESC NULLS LAST : le match le plus récent d'abord
--      NULLS LAST écrit en clair : Postgres met les NULL EN PREMIER
--      avec DESC, DuckDB en dernier -> on impose le même ordre (dev = prod)
--   3. nom_equipe                 : dernier départage, stable
-- La ligne gagnante de chaque équipe reçoit rang = 1
-- ---------------------------------------------------------------------
classement AS (
    SELECT
        id_equipe,
        nom_equipe,
        url_logo_equipe,
        ROW_NUMBER() OVER (
            PARTITION BY id_equipe
            ORDER BY
                CASE source
                    WHEN 'top'    THEN 1
                    WHEN 'matchs' THEN 2
                END,
                date_match DESC NULLS LAST,
                nom_equipe
        ) AS rang
    FROM toutes_les_equipes
    WHERE id_equipe IS NOT NULL          -- sécurité : pas de clé vide dans une dimension
)

-- ---------------------------------------------------------------------
-- Résultat : 1 ligne par équipe (la gagnante), colonnes validées
-- ---------------------------------------------------------------------
SELECT
    id_equipe,          -- clé de la dimension (reliée à id_equipe des faits)
    nom_equipe,
    url_logo_equipe     -- URL de l'image (Power BI : catégorie « URL d'image »)
FROM classement
WHERE rang = 1