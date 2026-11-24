-- =====================================================================
-- dwh_silver.fait_stats_joueur_saison
-- Fait STATS JOUEUR (projet 4) :
--   1 ligne par joueur × ligue × saison  (clé de la table)
--   id_equipe = le club retenu pour la saison (attribut, voir CTE 8)
-- Clés vers les dimensions (relations 1 -> plusieurs) :
--   id_joueur -> dim_joueur | id_equipe -> dim_equipe
--   id_ligue  -> dim_ligue  | saison    -> dim_saison
-- Contenu : colonnes brutes validées (les mesures/ratios se font dans
--           Power BI) + colonnes calculées ici (transformations) :
--   dans_top_buteurs / passeurs / cartons_jaunes / cartons_rouges,
--   age_saison, stats_suspectes, club_incertain
-- Règle de FUSION (validée) quand un joueur a plusieurs lignes
-- pour la même ligue et la même saison :
--   1. vrai transfert (somme des matchs <= maximum de la ligue) :
--      on ADDITIONNE matchs, titularisations, minutes, buts, cartons ;
--      les autres stats (recopiées par l'API) sont prises UNE fois
--   2. copie de la saison sous un autre club (somme > maximum) :
--      on garde UNE seule ligne
--   3. club retenu = celui avec le plus de minutes
--      (égalité -> plus de matchs, puis nom du club) ;
--      club_incertain = TRUE si plusieurs clubs ont le même maximum de minutes
-- Sources : les 4 listes top* + fixtures (maximum de matchs)
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : les 4 listes l'une sous l'autre (1 440 lignes)
--   liste    : de quelle liste vient la ligne (pour les booléens, CTE 2)
--   priorite : choix U = les 3 autres listes (1) avant topscorers (2),
--              car topscorers a des totaux impossibles (club, doublés)
-- ---------------------------------------------------------------------
WITH listes AS (
    SELECT 'buteurs' AS liste, 2 AS priorite, *
    FROM {{ ref('api_football_topscorers') }}
    UNION ALL
    SELECT 'passeurs' AS liste, 1 AS priorite, *
    FROM {{ ref('api_football_topassists') }}
    UNION ALL
    SELECT 'cartons_jaunes' AS liste, 1 AS priorite, *
    FROM {{ ref('api_football_topyellowcards') }}
    UNION ALL
    SELECT 'cartons_rouges' AS liste, 1 AS priorite, *
    FROM {{ ref('api_football_topredcards') }}
),

-- ---------------------------------------------------------------------
-- CTE 2 : dans quelles listes est le joueur ? (1 ligne par JOUEUR × LIGUE × SAISON)
--   CASE donne 1 si la ligne vient de la liste, sinon 0
--   MAX(...) = 1 : vrai si AU MOINS une ligne vient de la liste
--   (calculé sur toutes les équipes du joueur : la fusion ne perd rien)
-- ---------------------------------------------------------------------
appartenance AS (
    SELECT
        id_joueur,
        id_ligue,
        saison,
        MAX(CASE WHEN liste = 'buteurs'        THEN 1 ELSE 0 END) = 1 AS dans_top_buteurs,
        MAX(CASE WHEN liste = 'passeurs'       THEN 1 ELSE 0 END) = 1 AS dans_top_passeurs,
        MAX(CASE WHEN liste = 'cartons_jaunes' THEN 1 ELSE 0 END) = 1 AS dans_top_cartons_jaunes,
        MAX(CASE WHEN liste = 'cartons_rouges' THEN 1 ELSE 0 END) = 1 AS dans_top_cartons_rouges
    FROM listes
    GROUP BY id_joueur, id_ligue, saison
),

-- ---------------------------------------------------------------------
-- CTE 3 : UNE ligne de stats par joueur × ligue × saison × ÉQUIPE
--   (un joueur présent dans plusieurs listes n'est gardé qu'une fois)
--   1. priorite ASC             : les 3 autres listes avant topscorers
--   2. _date_depot DESC         : le dépôt le plus récent
--      NULLS LAST écrit en clair : même ordre en DuckDB et en Postgres
--   3. liste                    : dernier départage, stable
-- ---------------------------------------------------------------------
classement AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_joueur, id_ligue, saison, id_equipe
            ORDER BY priorite, _date_depot DESC NULLS LAST, liste
        ) AS rang
    FROM listes
),

-- ---------------------------------------------------------------------
-- CTE 4 : nombre de matchs joués par chaque équipe, par ligue et saison
--   Un match compte pour ses 2 équipes : domicile PUIS extérieur
-- ---------------------------------------------------------------------
matchs_par_equipe AS (
    SELECT id_ligue, saison, id_equipe, COUNT(*) AS nb_matchs_equipe
    FROM (
        SELECT id_ligue, saison, id_equipe_domicile  AS id_equipe
        FROM {{ ref('api_football_fixtures') }}
        UNION ALL
        SELECT id_ligue, saison, id_equipe_exterieur AS id_equipe
        FROM {{ ref('api_football_fixtures') }}
    ) AS equipes_des_matchs
    GROUP BY id_ligue, saison, id_equipe
),

-- ---------------------------------------------------------------------
-- CTE 5 : le MAXIMUM de matchs d'une équipe, par ligue et saison
--   (barrages compris) : 38 Premier League, 36 Bundesliga, 14 à 17 en C1
--   Un joueur ne peut pas avoir joué plus de matchs que ce maximum
-- ---------------------------------------------------------------------
max_matchs AS (
    SELECT id_ligue, saison, MAX(nb_matchs_equipe) AS max_matchs
    FROM matchs_par_equipe
    GROUP BY id_ligue, saison
),

-- ---------------------------------------------------------------------
-- CTE 6 : les lignes retenues (rang = 1) + le maximum de la ligue/saison
--   LEFT JOIN : si une ligue/saison n'avait aucun match, on garde la ligne
-- ---------------------------------------------------------------------
lignes AS (
    SELECT
        c.*,
        m.max_matchs
    FROM classement AS c
    LEFT JOIN max_matchs AS m
        ON  m.id_ligue = c.id_ligue
        AND m.saison   = c.saison
    WHERE c.rang = 1
),

-- ---------------------------------------------------------------------
-- CTE 7 : préparer la fusion, sans encore regrouper
--   Fonctions de fenêtre (OVER) : calcul sur toutes les lignes du même
--   joueur × ligue × saison, mais chaque ligne est conservée
--   rang_club     : 1 = le club retenu (plus de minutes, puis plus de
--                   matchs, puis nom du club)
--   nb_lignes     : 1 = pas de fusion ; 2 ou 3 = fusion
--   somme_matchs  : pour distinguer vrai transfert / copie (CTE 8)
--   max_minutes   : le plus grand nombre de minutes du joueur
-- ---------------------------------------------------------------------
preparation AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_joueur, id_ligue, saison
            ORDER BY minutes_jouees DESC NULLS LAST,
                     nb_matchs_joues DESC NULLS LAST,
                     nom_equipe
        )                                                         AS rang_club,
        COUNT(*)             OVER (PARTITION BY id_joueur, id_ligue, saison) AS nb_lignes,
        SUM(nb_matchs_joues) OVER (PARTITION BY id_joueur, id_ligue, saison) AS somme_matchs,
        MAX(minutes_jouees)  OVER (PARTITION BY id_joueur, id_ligue, saison) AS max_minutes
    FROM lignes
),

-- ---------------------------------------------------------------------
-- CTE 8 : type de chaque groupe joueur × ligue × saison
--   unique    : une seule ligne (cas général)
--   transfert : plusieurs clubs, somme des matchs possible -> on additionne
--   copie     : somme des matchs impossible -> la saison a été recopiée
--               sous un autre club -> on garde le club retenu seulement
--   nb_clubs_max_minutes : combien de clubs ont le maximum de minutes
--                          (> 1 = égalité -> club_incertain)
-- ---------------------------------------------------------------------
typage AS (
    SELECT
        *,
        CASE
            WHEN nb_lignes = 1                THEN 'unique'
            WHEN somme_matchs <= max_matchs   THEN 'transfert'
            ELSE                                   'copie'
        END                                                       AS type_groupe,
        SUM(CASE WHEN minutes_jouees = max_minutes THEN 1 ELSE 0 END)
            OVER (PARTITION BY id_joueur, id_ligue, saison)       AS nb_clubs_max_minutes
    FROM preparation
),

-- ---------------------------------------------------------------------
-- CTE 9 : la fusion : 1 ligne par joueur × ligue × saison
--   Colonnes RÉPARTIES par club (matchs, titularisations, minutes, buts,
--   cartons) : SUM si vrai transfert, sinon la valeur du club retenu
--   Colonnes RECOPIÉES par l'API (tirs, passes, duels...) : toujours
--   la valeur du club retenu, prise une seule fois
--   MAX(CASE WHEN rang_club = 1 THEN x END) = « x du club retenu »
--   (une seule ligne a rang_club = 1 ; MAX sert juste à regrouper)
-- ----------------------------------------------------------------------
fusion AS (
    SELECT
        id_joueur,
        id_ligue,
        saison,
        MAX(type_groupe)                                          AS type_groupe,
        MAX(max_matchs)                                           AS max_matchs,
        MAX(CASE WHEN rang_club = 1 THEN id_equipe END)           AS id_equipe,
        MAX(CASE WHEN rang_club = 1 THEN date_naissance END)      AS date_naissance,
        MAX(nb_lignes)                                            AS nb_lignes,
        MAX(nb_clubs_max_minutes)                                 AS nb_clubs_max_minutes,

        -- ----- Colonnes réparties par club -----
        CASE WHEN MAX(type_groupe) = 'transfert' THEN SUM(nb_matchs_joues)
             ELSE MAX(CASE WHEN rang_club = 1 THEN nb_matchs_joues END) END    AS nb_matchs_joues,
        CASE WHEN MAX(type_groupe) = 'transfert' THEN SUM(nb_titularisations)
             ELSE MAX(CASE WHEN rang_club = 1 THEN nb_titularisations END) END AS nb_titularisations,
        CASE WHEN MAX(type_groupe) = 'transfert' THEN SUM(minutes_jouees)
             ELSE MAX(CASE WHEN rang_club = 1 THEN minutes_jouees END) END     AS minutes_jouees,
        CASE WHEN MAX(type_groupe) = 'transfert' THEN SUM(nb_buts)
             ELSE MAX(CASE WHEN rang_club = 1 THEN nb_buts END) END            AS nb_buts,
        CASE WHEN MAX(type_groupe) = 'transfert' THEN SUM(nb_cartons_jaunes)
             ELSE MAX(CASE WHEN rang_club = 1 THEN nb_cartons_jaunes END) END  AS nb_cartons_jaunes,
        CASE WHEN MAX(type_groupe) = 'transfert' THEN SUM(nb_cartons_rouges)
             ELSE MAX(CASE WHEN rang_club = 1 THEN nb_cartons_rouges END) END  AS nb_cartons_rouges,

        -- ----- Colonnes recopiées : valeur du club retenu, une seule fois -----
        MAX(CASE WHEN rang_club = 1 THEN note_moyenne END)        AS note_moyenne,
        MAX(CASE WHEN rang_club = 1 THEN nb_passes_decisives END) AS nb_passes_decisives,
        MAX(CASE WHEN rang_club = 1 THEN nb_tirs END)             AS nb_tirs,
        MAX(CASE WHEN rang_club = 1 THEN nb_tirs_cadres END)      AS nb_tirs_cadres,
        MAX(CASE WHEN rang_club = 1 THEN nb_passes_cles END)      AS nb_passes_cles,
        MAX(CASE WHEN rang_club = 1 THEN nb_dribbles_tentes END)  AS nb_dribbles_tentes,
        MAX(CASE WHEN rang_club = 1 THEN nb_dribbles_reussis END) AS nb_dribbles_reussis,
        MAX(CASE WHEN rang_club = 1 THEN nb_tacles END)           AS nb_tacles,
        MAX(CASE WHEN rang_club = 1 THEN nb_contres END)          AS nb_contres,
        MAX(CASE WHEN rang_club = 1 THEN nb_interceptions END)    AS nb_interceptions,
        MAX(CASE WHEN rang_club = 1 THEN nb_duels END)            AS nb_duels,
        MAX(CASE WHEN rang_club = 1 THEN nb_duels_gagnes END)     AS nb_duels_gagnes,
        MAX(CASE WHEN rang_club = 1 THEN nb_fautes_commises END)  AS nb_fautes_commises,
        MAX(CASE WHEN rang_club = 1 THEN nb_penaltys_marques END) AS nb_penaltys_marques,
        MAX(CASE WHEN rang_club = 1 THEN nb_penaltys_manques END) AS nb_penaltys_manques
    FROM typage
    GROUP BY id_joueur, id_ligue, saison
)

-- ---------------------------------------------------------------------
-- Résultat : 1 ligne par joueur × ligue × saison
-- ---------------------------------------------------------------------
SELECT
    -- ----- Clés vers les dimensions -----
    f.id_joueur,
    f.id_equipe,                                     -- club retenu (règle 3)
    f.id_ligue,
    f.saison,

    -- ----- Âge atteint dans l'année de la saison (choix validé) -----
    -- EXTRACT(YEAR FROM date) = l'année de naissance ; CAST : même type dev/prod
    f.saison - CAST(EXTRACT(YEAR FROM f.date_naissance) AS INTEGER) AS age_saison,

    -- ----- Colonnes brutes validées (les mesures se font dans Power BI) -----
    -- Temps de jeu
    f.nb_matchs_joues,
    f.nb_titularisations,
    f.minutes_jouees,
    f.note_moyenne,
    -- Attaque
    f.nb_buts,
    f.nb_passes_decisives,
    f.nb_tirs,
    f.nb_tirs_cadres,
    f.nb_passes_cles,
    f.nb_dribbles_tentes,
    f.nb_dribbles_reussis,
    -- Défense
    f.nb_tacles,
    f.nb_contres,
    f.nb_interceptions,
    f.nb_duels,
    f.nb_duels_gagnes,
    -- Discipline
    f.nb_fautes_commises,
    f.nb_cartons_jaunes,
    f.nb_cartons_rouges,
    -- Penaltys
    f.nb_penaltys_marques,
    f.nb_penaltys_manques,

    -- ----- Appartenance aux listes (CTE 2) -----
    a.dans_top_buteurs,
    a.dans_top_passeurs,
    a.dans_top_cartons_jaunes,
    a.dans_top_cartons_rouges,

    -- ----- Ligne suspecte : plus de matchs que le maximum possible -----
    -- (calculé APRÈS la fusion, sur les matchs de la ligne finale)
    CASE
        WHEN f.nb_matchs_joues > f.max_matchs THEN TRUE
        ELSE FALSE
    END                                              AS stats_suspectes,

    -- ----- Club incertain : plusieurs clubs avec le même maximum de minutes -----
    CASE
        WHEN f.nb_lignes > 1 AND f.nb_clubs_max_minutes > 1 THEN TRUE
        ELSE FALSE
    END                                              AS club_incertain

FROM fusion AS f
INNER JOIN appartenance AS a
    ON  a.id_joueur = f.id_joueur
    AND a.id_ligue  = f.id_ligue
    AND a.saison    = f.saison