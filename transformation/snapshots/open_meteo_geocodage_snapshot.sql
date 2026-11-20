{#
  =====================================================================
  dwh_snapshot.open_meteo_geocodage_snapshot — historique SCD2 du géocodage
  ---------------------------------------------------------------------
  Lit le BRONZE BRUT (aucun renommage : on historise la donnée telle quelle).
  À chaque "dbt snapshot", pour chaque ville (_ville_fixture) :
    - on garde le dépôt le plus récent du bronze (CTE classement)
    - si son _date_depot est plus récent que la version en cours du snapshot
        -> l'ancienne version est FERMÉE (dbt_valid_to rempli)
        -> la nouvelle est ajoutée (dbt_valid_to vide = version en cours)
  Les modèles dwh_bronze lisent ce snapshot avec WHERE dbt_valid_to IS NULL.
  =====================================================================
#}
{% snapshot open_meteo_geocodage_snapshot %}

{#
  Réglages (pas de commentaire "--" À L'INTÉRIEUR de config(...) : c'est du Jinja)
    unique_key : la ville du match = 1 historique par ville
    strategy   : 'timestamp' = nouvelle version si updated_at est plus récent
    updated_at : la date du dépôt bronze
#}
{{
    config(
        unique_key='_ville_fixture',
        strategy='timestamp',
        updated_at='_date_depot'
    )
}}

-- ---------------------------------------------------------------------
-- 1. Classement des dépôts de chaque ville : n°1 = le plus récent
--    (le bronze garde TOUS les dépôts -> plusieurs lignes par ville)
-- ---------------------------------------------------------------------
WITH classement AS (
    SELECT
        _ville_fixture,
        _fichier_source,
        ROW_NUMBER() OVER (
            PARTITION BY _ville_fixture
            ORDER BY _date_depot DESC, _fichier_source DESC
        ) AS rang
    FROM {{ source('bronze', 'open_meteo_geocodage') }}
)

-- ---------------------------------------------------------------------
-- 2. On ne garde que la ligne n°1 de chaque ville, avec TOUTES ses colonnes brutes
--    (une ville + un fichier = une seule ligne dans le bronze)
-- ---------------------------------------------------------------------
SELECT b.*
FROM {{ source('bronze', 'open_meteo_geocodage') }} AS b
JOIN classement AS c
  ON  c._ville_fixture  = b._ville_fixture
  AND c._fichier_source = b._fichier_source
WHERE c.rang = 1

{% endsnapshot %}