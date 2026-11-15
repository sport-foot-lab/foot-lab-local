{#
  =====================================================================
  nettoyer_texte(colonne) : normalise une colonne texte
  ---------------------------------------------------------------------
  Étapes (de l'intérieur vers l'extérieur) :
    1. REGEXP_REPLACE(..., '\s+', ' ', 'g')
         \s+ = un paquet de "blancs" (espace, tabulation, retour ligne)
         ' ' = remplacé par UN seul espace
         'g' = global : tous les paquets, pas seulement le 1er
               (obligatoire en Postgres, accepté par DuckDB)
    2. TRIM(...)        : retire les espaces au début et à la fin
    3. NULLIF(..., '')  : un texte vide devient un vrai NULL
  Même SQL en dev (DuckDB) et en prod (Postgres).
  Usage dans un modèle : {{ nettoyer_texte('teams__home__name') }}
  =====================================================================
#}

{# macro = début de la "fonction" ; colonne = son paramètre (un nom de colonne, en texte) #}
{% macro nettoyer_texte(colonne) -%}
    NULLIF(TRIM(REGEXP_REPLACE({{ colonne }}, '\s+', ' ', 'g')), '')
{%- endmacro %}
{# {{ colonne }} est remplacé par la valeur passée, ex. teams__home__name   #}
{# Les "-" de -%} et {%- retirent les sauts de ligne autour du SQL généré  #}