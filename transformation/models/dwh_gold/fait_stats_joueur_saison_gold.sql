{#
  =====================================================================
  dwh_gold.fait_stats_joueur_saison  (fichier : fait_stats_joueur_saison_gold.sql)
  Couche GOLD = COPIE du silver, lue par Power BI. Aucune transformation.
  Nom du FICHIER  : fait_stats_joueur_saison_gold
                    (fait_stats_joueur_saison existe déjà en silver)
  Nom de la TABLE : fait_stats_joueur_saison grâce à alias
  Grain : 1 ligne par joueur × ligue × saison
  Les 38 lignes stats_suspectes = TRUE sont GARDÉES (choix S-b) :
  Power BI les écarte avec un filtre stats_suspectes = Faux
  Colonnes listées une par une = « contrat » stable pour Power BI
  =====================================================================
#}
{{ config(alias='fait_stats_joueur_saison') }}

SELECT
    -- ----- Clés vers les dimensions (relations 1 -> plusieurs) -----
    id_joueur,              -- -> dim_joueur
    id_equipe,              -- -> dim_equipe (club retenu pour la saison)
    id_ligue,               -- -> dim_ligue
    saison,                 -- -> dim_saison

    -- ----- Âge atteint dans l'année de la saison -----
    age_saison,

    -- ----- Colonnes brutes (les mesures se font en DAX dans Power BI) -----
    -- Temps de jeu
    nb_matchs_joues,
    nb_titularisations,
    minutes_jouees,
    note_moyenne,
    -- Attaque
    nb_buts,
    nb_passes_decisives,
    nb_tirs,
    nb_tirs_cadres,
    nb_passes_cles,
    nb_dribbles_tentes,
    nb_dribbles_reussis,
    -- Défense
    nb_tacles,
    nb_contres,
    nb_interceptions,
    nb_duels,
    nb_duels_gagnes,
    -- Discipline
    nb_fautes_commises,
    nb_cartons_jaunes,
    nb_cartons_rouges,
    -- Penaltys
    nb_penaltys_marques,
    nb_penaltys_manques,

    -- ----- Indicateurs (filtres / segments dans Power BI) -----
    dans_top_buteurs,
    dans_top_passeurs,
    dans_top_cartons_jaunes,
    dans_top_cartons_rouges,
    stats_suspectes,        -- filtre Power BI : stats_suspectes = Faux
    club_incertain          -- TRUE = club choisi par départage (minutes égales)
FROM {{ ref('fait_stats_joueur_saison') }}