{#
  =====================================================================
  dwh_gold.dim_saison  (fichier : dim_saison_gold.sql)
  Couche GOLD = COPIE du silver, lue par Power BI. Aucune transformation.
  Nom du FICHIER  : dim_saison_gold  (dim_saison existe déjà en silver)
  Nom de la TABLE : dim_saison grâce à alias -> dwh_gold.dim_saison
  Dimension PARTAGÉE : UN seul segment « saison » pour tous les faits
  Colonnes listées une par une = « contrat » stable pour Power BI
  =====================================================================
#}
{{ config(alias='dim_saison') }}

SELECT
    saison,             -- clé de la dimension (relation 1 -> plusieurs vers les faits)
    libelle_saison      -- ex. '2022-2023' (pour les segments et les axes)
FROM {{ ref('dim_saison') }}