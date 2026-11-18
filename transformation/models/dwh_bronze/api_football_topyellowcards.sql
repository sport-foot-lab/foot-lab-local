-- =====================================================================
-- dwh_bronze.api_football_topyellowcards
-- Meilleurs cartons jaunes : 1 ligne par joueur, ligue, saison, équipe ET dépôt
-- (un joueur transféré en cours de saison a 1 ligne par équipe)
-- Renommage FR, typage, nettoyage, flag est_derniere_version
-- =====================================================================

-- ---------------------------------------------------------------------
-- CTE 1 : la source brute, rien d'autre
-- ---------------------------------------------------------------------
WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'api_football_topyellowcards') }}
),

-- ---------------------------------------------------------------------
-- CTE 2 : nettoyage des textes qui contiennent des nombres / dates
-- On retire l'unité ("170 cm" -> "170", "68 kg" -> "68")
-- \s* = 0, 1 ou plusieurs blancs ; $ = fin du texte
-- ---------------------------------------------------------------------
nettoye AS (
    SELECT
        *,
        REGEXP_REPLACE({{ nettoyer_texte('player__height') }}, '\s*cm$', '') AS taille_texte,
        REGEXP_REPLACE({{ nettoyer_texte('player__weight') }}, '\s*kg$', '') AS poids_texte
    FROM source
),

-- ---------------------------------------------------------------------
-- CTE 3 : renommage FR + typage
--   CAST         : colonne déjà numérique en bronze (BIGINT) -> type choisi
--   essayer_cast : colonne TEXTE en bronze -> nombre / date, NULL si mal formé
-- ---------------------------------------------------------------------
renomme AS (
    SELECT
        -- ----- Joueur -----
        CAST(player__id AS BIGINT)                                AS id_joueur,
        {{ nettoyer_texte('player__name') }}                      AS nom_joueur,
        {{ nettoyer_texte('player__firstname') }}                 AS prenom_joueur,
        {{ nettoyer_texte('player__lastname') }}                  AS nom_famille_joueur,
        CAST(player__age AS INTEGER)                              AS age_joueur,
        {{ essayer_cast('player__birth__date', 'DATE') }}         AS date_naissance,
        {{ nettoyer_texte('player__birth__place') }}              AS lieu_naissance,
        {{ nettoyer_texte('player__birth__country') }}            AS pays_naissance,
        {{ nettoyer_texte('player__nationality') }}               AS nationalite,
        {{ essayer_cast('taille_texte', 'INTEGER') }}             AS taille_cm,
        {{ essayer_cast('poids_texte', 'INTEGER') }}              AS poids_kg,
        player__injured                                           AS est_blesse,
        player__photo                                             AS url_photo_joueur,

        -- ----- Équipe -----
        CAST(statistics__team__id AS BIGINT)                      AS id_equipe,
        {{ nettoyer_texte('statistics__team__name') }}            AS nom_equipe,
        statistics__team__logo                                    AS url_logo_equipe,

        -- ----- Ligue / saison -----
        CAST(statistics__league__id AS BIGINT)                    AS id_ligue,
        {{ nettoyer_texte('statistics__league__name') }}          AS nom_ligue,
        {{ nettoyer_texte('statistics__league__country') }}       AS pays_ligue,
        CAST(statistics__league__season AS INTEGER)               AS saison,
        statistics__league__logo                                  AS url_logo_ligue,
        statistics__league__flag                                  AS url_drapeau_ligue,

        -- ----- Temps de jeu -----
        {{ nettoyer_texte('statistics__games__position') }}       AS poste,
        CAST(statistics__games__number AS INTEGER)                AS numero_maillot,
        statistics__games__captain                                AS est_capitaine,
        CAST(statistics__games__appearences AS INTEGER)           AS nb_matchs_joues,
        CAST(statistics__games__lineups AS INTEGER)               AS nb_titularisations,
        CAST(statistics__games__minutes AS INTEGER)               AS minutes_jouees,
        {{ essayer_cast('statistics__games__rating', 'DECIMAL(10,6)') }} AS note_moyenne,
        CAST(statistics__substitutes__in AS INTEGER)              AS nb_entrees_en_jeu,
        CAST(statistics__substitutes__out AS INTEGER)             AS nb_sorties_en_jeu,
        CAST(statistics__substitutes__bench AS INTEGER)           AS nb_matchs_sur_banc,

        -- ----- Attaque -----
        CAST(statistics__goals__total AS INTEGER)                 AS nb_buts,
        CAST(statistics__goals__assists AS INTEGER)               AS nb_passes_decisives,
        CAST(statistics__shots__total AS INTEGER)                 AS nb_tirs,
        CAST(statistics__shots__on AS INTEGER)                    AS nb_tirs_cadres,
        CAST(statistics__passes__total AS INTEGER)                AS nb_passes,
        CAST(statistics__passes__key AS INTEGER)                  AS nb_passes_cles,
        CAST(statistics__passes__accuracy AS INTEGER)             AS precision_passes,  -- valeur brute API passes.accuracy
        CAST(statistics__dribbles__attempts AS INTEGER)           AS nb_dribbles_tentes,
        CAST(statistics__dribbles__success AS INTEGER)            AS nb_dribbles_reussis,
        CAST(statistics__dribbles__past AS INTEGER)               AS dribbles_past,     -- valeur brute API dribbles.past

        -- ----- Défense / gardien -----
        CAST(statistics__tackles__total AS INTEGER)               AS nb_tacles,
        CAST(statistics__tackles__blocks AS INTEGER)              AS nb_contres,
        CAST(statistics__tackles__interceptions AS INTEGER)       AS nb_interceptions,
        CAST(statistics__duels__total AS INTEGER)                 AS nb_duels,
        CAST(statistics__duels__won AS INTEGER)                   AS nb_duels_gagnes,
        CAST(statistics__goals__conceded AS INTEGER)              AS nb_buts_encaisses,
        CAST(statistics__goals__saves AS INTEGER)                 AS nb_arrets,

        -- ----- Discipline -----
        CAST(statistics__fouls__drawn AS INTEGER)                 AS nb_fautes_subies,
        CAST(statistics__fouls__committed AS INTEGER)             AS nb_fautes_commises,
        CAST(statistics__cards__yellow AS INTEGER)                AS nb_cartons_jaunes,
        CAST(statistics__cards__yellowred AS INTEGER)             AS nb_cartons_jaune_rouge,
        CAST(statistics__cards__red AS INTEGER)                   AS nb_cartons_rouges,

        -- ----- Penaltys -----
        CAST(statistics__penalty__won AS INTEGER)                 AS nb_penaltys_obtenus,
        CAST(statistics__penalty__commited AS INTEGER)            AS nb_penaltys_commis,
        CAST(statistics__penalty__scored AS INTEGER)              AS nb_penaltys_marques,
        CAST(statistics__penalty__missed AS INTEGER)              AS nb_penaltys_manques,
        CAST(statistics__penalty__saved AS INTEGER)               AS nb_penaltys_arretes,

        -- ----- Colonnes techniques (traçabilité, gardées telles quelles) -----
        _date_depot,
        _fichier_source,
        _date_chargement_bronze,
        _dlt_load_id                                              AS _id_chargement_dlt
    FROM nettoye
),

-- ---------------------------------------------------------------------
-- CTE 4 : flag de version (aucun dépôt supprimé)
-- Clé = joueur + ligue + saison + équipe
-- ---------------------------------------------------------------------
final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_joueur, id_ligue, saison, id_equipe
            ORDER BY _date_depot DESC,
                     _fichier_source DESC
        ) = 1                                                     AS est_derniere_version
    FROM renomme
)

SELECT * FROM final