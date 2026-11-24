{#
  =====================================================================
  dwh_gold.dim_joueur  (fichier : dim_joueur_gold.sql)
  Couche GOLD = COPIE du silver, lue par Power BI. Aucune transformation.
  Nom du FICHIER  : dim_joueur_gold  (dbt exige un nom de modèle unique :
                    dim_joueur existe déjà en silver)
  Nom de la TABLE : dim_joueur grâce à alias -> dwh_gold.dim_joueur
  Colonnes listées une par une = « contrat » stable pour Power BI :
  une colonne ajoutée en silver n'arrive ici que si on l'ajoute.
  =====================================================================
#}
{{ config(alias='dim_joueur') }}

SELECT
    id_joueur,          -- clé de la dimension (relation 1 -> plusieurs vers le fait)
    nom_joueur,
    nationalite,
    date_naissance,
    lieu_naissance,     -- 'Inconnu' si absent (règle silver)
    taille_cm,
    url_photo_joueur,   -- Power BI : catégorie de données « URL d'image »
    poste
FROM {{ ref('dim_joueur') }}