{#
  =====================================================================
  dwh_snapshot.open_meteo_meteo_journaliere_snapshot — historique SCD2 de la météo journalière
  ---------------------------------------------------------------------
  Lit le BRONZE BRUT (aucun renommage : on historise la donnée telle quelle).
  Clé = coordonnées demandées + jour (_param_latitude, _param_longitude, time).
  À chaque "dbt snapshot", pour chaque clé :
    - on garde le dépôt le plus récent du bronze (CTE classement)
    - si son dépôt est plus récent que la version en cours du snapshot
        -> l'ancienne version est FERMÉE (dbt_valid_to rempli)
        -> la nouvelle est ajoutée (dbt_valid_to vide = version en cours)
  Le modèle dwh_bronze lit ce snapshot avec WHERE dbt_valid_to IS NULL.
  =====================================================================
#}
{% snapshot open_meteo_meteo_journaliere_snapshot %}

{#
  Réglages (pas de commentaire "--" À L'INTÉRIEUR de config(...) : c'est du Jinja)
    unique_key : clé à 3 colonnes -> une LISTE
    strategy   : 'timestamp' = nouvelle version si updated_at est plus récent
    updated_at : la date du dépôt en UTC sans fuseau (évite le WARNING de type)
#}
{{
    config(
        unique_key=['_param_latitude', '_param_longitude', 'time'],
        strategy='timestamp',
        updated_at='_date_depot_utc'
    )
}}

-- ---------------------------------------------------------------------
-- 1. Classement des dépôts de chaque clé : n°1 = le plus récent
-- ---------------------------------------------------------------------
WITH classement AS (
    SELECT
        _param_latitude,
        _param_longitude,
        time,
        _fichier_source,
        ROW_NUMBER() OVER (
            PARTITION BY _param_latitude, _param_longitude, time
            ORDER BY _date_depot DESC, _fichier_source DESC
        ) AS rang
    FROM {{ source('bronze', 'open_meteo_meteo_journaliere') }}
)

-- ---------------------------------------------------------------------
-- 2. On ne garde que la ligne n°1 de chaque clé, avec TOUTES ses colonnes brutes
--    (une clé + un fichier = une seule ligne dans le bronze)
-- ---------------------------------------------------------------------
SELECT
    b.*,                                                                       -- toutes les colonnes brutes
    CAST(b._date_depot AT TIME ZONE 'UTC' AS TIMESTAMP) AS _date_depot_utc     -- date du dépôt, UTC sans fuseau
FROM {{ source('bronze', 'open_meteo_meteo_journaliere') }} AS b
JOIN classement AS c
  ON  c._param_latitude  = b._param_latitude
  AND c._param_longitude = b._param_longitude
  AND c.time             = b.time
  AND c._fichier_source  = b._fichier_source
WHERE c.rang = 1

{% endsnapshot %}