-- =====================================================================
-- Test singulier : fait_stats_joueur_saison_cle_unique
-- Vérifie que la clé du fait (joueur × ligue × saison) n'a PAS de doublon
-- Principe d'un test singulier dbt :
--   la requête cherche les lignes EN ERREUR
--   0 ligne renvoyée = test RÉUSSI ; au moins 1 ligne = test ÉCHOUÉ
-- (dbt n'a pas de test intégré pour une clé sur plusieurs colonnes)
-- =====================================================================

SELECT
    id_joueur,
    id_ligue,
    saison,
    COUNT(*) AS nb_lignes                         -- combien de fois la clé apparaît
FROM {{ ref('fait_stats_joueur_saison') }}
GROUP BY id_joueur, id_ligue, saison
HAVING COUNT(*) > 1                               -- on ne garde que les clés en double