{#
  =====================================================================
  dwh_snapshot.api_football_fixtures_snapshot — historique SCD2 des matchs
  ---------------------------------------------------------------------
  Lit le BRONZE BRUT (aucun renommage : on historise la donnée telle quelle).
  À chaque "dbt snapshot", pour chaque match (fixture__id) :
    - on garde le dépôt le plus récent du bronze (CTE classement)
    - si son _date_depot est plus récent que la version en cours du snapshot
        -> l'ancienne version est FERMÉE (dbt_valid_to rempli)
        -> la nouvelle est ajoutée (dbt_valid_to vide = version en cours)
  Le modèle dwh_bronze lit ce snapshot avec WHERE dbt_valid_to IS NULL.
  =====================================================================
#}


{% snapshot api_football_fixtures_snapshot %}

{#
  Réglages (pas de commentaire "--" À L'INTÉRIEUR de config(...) : c'est du Jinja)
    unique_key : l'identifiant du match = 1 historique par match
    strategy   : 'timestamp' = nouvelle version si updated_at est plus récent
    updated_at : la date du dépôt bronze
#}

{{
    config(
        unique_key='fixture__id',
        strategy='timestamp',
        updated_at='_date_depot_utc'
    )
}}

-- ---------------------------------------------------------------------
-- 1. Classement des dépôts de chaque match : n°1 = le plus récent
-- ---------------------------------------------------------------------
WITH classement AS (
    SELECT
        fixture__id,
        _fichier_source,
        ROW_NUMBER() OVER (
            PARTITION BY fixture__id
            ORDER BY _date_depot DESC, _fichier_source DESC
        ) AS rang
    FROM {{ source('bronze', 'api_football_fixtures') }}
)

-- ---------------------------------------------------------------------
-- 2. On ne garde que la ligne n°1 de chaque match, avec TOUTES ses colonnes brutes
--    (un match + un fichier = une seule ligne dans le bronze)
-- ---------------------------------------------------------------------
SELECT b.*,
 CAST(b._date_depot AT TIME ZONE 'UTC' AS TIMESTAMP) AS _date_depot_utc   -- date du dépôt, UTC sans fuseau
FROM {{ source('bronze', 'api_football_fixtures') }} AS b
JOIN classement AS c
  ON  c.fixture__id     = b.fixture__id
  AND c._fichier_source = b._fichier_source
WHERE c.rang = 1

{% endsnapshot %}