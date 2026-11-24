{#
  =====================================================================
  dwh_gold.dim_ligue  (fichier : dim_ligue_gold.sql)
  Couche GOLD = COPIE du silver, lue par Power BI. Aucune transformation.
  Nom du FICHIER  : dim_ligue_gold  (dim_ligue existe déjà en silver)
  Nom de la TABLE : dim_ligue grâce à alias -> dwh_gold.dim_ligue
  Dimension PARTAGÉE (projet 4 aujourd'hui, projet 2 ensuite)
  Colonnes listées une par une = « contrat » stable pour Power BI
  =====================================================================
#}
{{ config(alias='dim_ligue') }}

SELECT
    id_ligue,           -- clé de la dimension (relation 1 -> plusieurs vers les faits)
    nom_ligue,
    url_logo_ligue      -- Power BI : catégorie de données « URL d'image »
FROM {{ ref('dim_ligue') }}