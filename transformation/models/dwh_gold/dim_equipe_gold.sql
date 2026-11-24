{#
  =====================================================================
  dwh_gold.dim_equipe  (fichier : dim_equipe_gold.sql)
  Couche GOLD = COPIE du silver, lue par Power BI. Aucune transformation.
  Nom du FICHIER  : dim_equipe_gold  (dim_equipe existe déjà en silver)
  Nom de la TABLE : dim_equipe grâce à alias -> dwh_gold.dim_equipe
  Dimension PARTAGÉE (projet 4 aujourd'hui, projet 2 ensuite)
  Colonnes listées une par une = « contrat » stable pour Power BI
  =====================================================================
#}
{{ config(alias='dim_equipe') }}

SELECT
    id_equipe,          -- clé de la dimension (relation 1 -> plusieurs vers les faits)
    nom_equipe,         -- un seul nom par équipe (règle E3 appliquée en silver)
    url_logo_equipe     -- Power BI : catégorie de données « URL d'image »
FROM {{ ref('dim_equipe') }}