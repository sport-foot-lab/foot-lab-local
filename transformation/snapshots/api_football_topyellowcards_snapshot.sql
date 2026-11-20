{#
  =====================================================================
  dwh_snapshot.api_football_topyellowcards — historique SCD2 des joeurs avec le plus de cartons jaunes
  ---------------------------------------------------------------------
  Lit le BRONZE BRUT (aucun renommage : on historise la donnée telle quelle).
  Clé = joueur + ligue + saison + équipe (un joueur transféré = 1 ligne par équipe).
  À chaque "dbt snapshot", pour chaque clé :
    - on garde le dépôt le plus récent du bronze (CTE classement)
    - si son dépôt est plus récent que la version en cours du snapshot
        -> l'ancienne version est FERMÉE (dbt_valid_to rempli)
        -> la nouvelle est ajoutée (dbt_valid_to vide = version en cours)
  Le modèle dwh_bronze lit ce snapshot avec WHERE dbt_valid_to IS NULL.
  =====================================================================
#}
{% snapshot api_football_topyellowcards_snapshot %}

{#
  Réglages (pas de commentaire "--" À L'INTÉRIEUR de config(...) : c'est du Jinja)
    unique_key : clé à 4 colonnes -> une LISTE
    strategy   : 'timestamp' = nouvelle version si updated_at est plus récent
    updated_at : la date du dépôt en UTC sans fuseau (évite le WARNING de type)
#}
{{
    config(
        unique_key=['player__id', 'statistics__league__id', 'statistics__league__season', 'statistics__team__id'],
        strategy='timestamp',
        updated_at='_date_depot_utc'
    )
}}

-- ---------------------------------------------------------------------
-- 1. Classement des dépôts de chaque clé : n°1 = le plus récent
-- ---------------------------------------------------------------------
WITH classement AS (
    SELECT
        player__id,
        statistics__league__id,
        statistics__league__season,
        statistics__team__id,
        _fichier_source,
        ROW_NUMBER() OVER (
            PARTITION BY player__id, statistics__league__id, statistics__league__season, statistics__team__id
            ORDER BY _date_depot DESC, _fichier_source DESC
        ) AS rang
    FROM {{ source('bronze', 'api_football_topyellowcards') }}
)

-- ---------------------------------------------------------------------
-- 2. On ne garde que la ligne n°1 de chaque clé, avec TOUTES ses colonnes brutes
--    (une clé + un fichier = une seule ligne dans le bronze)
-- ---------------------------------------------------------------------
SELECT
    b.*,                                                                       -- toutes les colonnes brutes
    CAST(b._date_depot AT TIME ZONE 'UTC' AS TIMESTAMP) AS _date_depot_utc     -- date du dépôt, UTC sans fuseau
FROM {{ source('bronze', 'api_football_topyellowcards') }} AS b
JOIN classement AS c
  ON  c.player__id                 = b.player__id
  AND c.statistics__league__id     = b.statistics__league__id
  AND c.statistics__league__season = b.statistics__league__season
  AND c.statistics__team__id       = b.statistics__team__id
  AND c._fichier_source            = b._fichier_source
WHERE c.rang = 1

{% endsnapshot %}


