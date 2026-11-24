-- =====================================================================
-- dwh_silver.dim_joueur
-- Dimension JOUEUR : 1 ligne par joueur (id_joueur = clé)
-- Sert à la fiche joueur (photo, taille, nationalité...) du projet 4
-- Source : les 4 listes top* de dwh_bronze (les seules à avoir les joueurs)
-- Pas d'âge ici : l'âge de l'API est le même pour toutes les saisons ;
--                 l'âge par saison sera calculé dans le fait
-- Règle si un joueur avait des infos différentes selon les lignes
-- (aucun cas aujourd'hui, vérifié sur les 831 joueurs) :
--   on garde la ligne du dépôt le plus récent
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : les joueurs des 4 listes, l'une sous l'autre
-- UNION ALL : on garde tout (1 440 lignes), la CTE 2 choisira
-- UNE ligne par joueur. _date_depot sert au départage.
-- ---------------------------------------------------------------------
WITH joueurs_top AS (
    SELECT id_joueur, nom_joueur, nationalite, date_naissance, lieu_naissance,
           taille_cm, url_photo_joueur, poste, _date_depot
    FROM {{ ref('api_football_topscorers') }}
    UNION ALL
    SELECT id_joueur, nom_joueur, nationalite, date_naissance, lieu_naissance,
           taille_cm, url_photo_joueur, poste, _date_depot
    FROM {{ ref('api_football_topassists') }}
    UNION ALL
    SELECT id_joueur, nom_joueur, nationalite, date_naissance, lieu_naissance,
           taille_cm, url_photo_joueur, poste, _date_depot
    FROM {{ ref('api_football_topyellowcards') }}
    UNION ALL
    SELECT id_joueur, nom_joueur, nationalite, date_naissance, lieu_naissance,
           taille_cm, url_photo_joueur, poste, _date_depot
    FROM {{ ref('api_football_topredcards') }}
),

-- ---------------------------------------------------------------------
-- CTE 2 : numéroter les lignes de chaque joueur
--   PARTITION BY id_joueur          : numérotation recommencée par joueur
--   _date_depot DESC NULLS LAST     : le dépôt le plus récent d'abord
--                                     (NULLS LAST écrit : même ordre dev/prod)
--   nom_joueur                      : dernier départage, stable
-- Un joueur présent dans plusieurs listes (ex. Savanier, 4 listes)
-- n'aura qu'UNE ligne avec rang = 1
-- ---------------------------------------------------------------------
classement AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_joueur
            ORDER BY _date_depot DESC NULLS LAST, nom_joueur
        ) AS rang
    FROM joueurs_top
    WHERE id_joueur IS NOT NULL          -- sécurité : pas de clé vide dans une dimension
)

-- ---------------------------------------------------------------------
-- Résultat : 1 ligne par joueur, colonnes validées
-- ---------------------------------------------------------------------
SELECT
    id_joueur,                                        -- clé (reliée à id_joueur du fait)
    nom_joueur,
    nationalite,
    date_naissance,
    -- Règle silver : texte NULL -> 'Inconnu'
    -- COALESCE(a, b) = a s'il n'est pas NULL, sinon b
    -- (équivaut à : CASE WHEN lieu_naissance IS NULL THEN 'Inconnu' ELSE lieu_naissance END)
    COALESCE(lieu_naissance, 'Inconnu')  AS lieu_naissance,
    taille_cm,                                        -- NULL gardé (choix validé : pas de 0 cm)
    url_photo_joueur,                                 -- URL de l'image (Power BI : « URL d'image »)
    poste                                             -- Attacker / Forward : décision reportée (G3)
FROM classement
WHERE rang = 1