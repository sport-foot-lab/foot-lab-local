{#
  =====================================================================
  essayer_cast(expression, type_sql) : CAST qui renvoie NULL au lieu de planter
  ---------------------------------------------------------------------
  Problème : DuckDB a TRY_CAST, Postgres 14 non.
             Et TRY_CAST seul n'a pas les mêmes règles que Postgres :
               TRY_CAST('6.9' AS INTEGER) -> 7 en DuckDB (arrondi !)
               CAST('6.9' AS INTEGER)     -> ERREUR en Postgres
  Solution : UNE règle commune = une expression régulière par famille de type.
             Le texte passe le contrôle -> on le convertit
             Sinon                      -> NULL
             -> mêmes résultats en dev et en prod.
  Familles gérées : entier, décimal, date (format AAAA-MM-JJ).
  Usage : {{ essayer_cast('player__height', 'INTEGER') }}
  =====================================================================
#}

{# ---------- 1. Point d'entrée : choisit la version selon la base ---------- #}
{# adapter.dispatch cherche <adapter>__essayer_cast : duckdb__… en dev,       #}
{# sinon default__… (Postgres en prod)                                        #}
{% macro essayer_cast(expression, type_sql) -%}
    {{ return(adapter.dispatch('essayer_cast')(expression, type_sql)) }}
{%- endmacro %}


{# ---------- 2. La règle commune : quelle forme de texte est acceptée ? ---------- #}
{% macro essayer_cast_motif(type_sql) -%}
    {#- famille = le type sans ses paramètres, en minuscules : 'DECIMAL(4,2)' -> 'decimal' -#}
    {%- set famille = type_sql.split('(')[0] | trim | lower -%}
    {%- if famille in ['integer', 'int', 'bigint', 'smallint'] -%}
        ^[+-]?[0-9]+$
    {%- elif famille in ['decimal', 'numeric', 'double', 'double precision', 'float', 'real'] -%}
        ^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$
    {%- elif famille == 'date' -%}
        ^[0-9]{4}-[0-9]{2}-[0-9]{2}$
    {%- else -%}
        {#- type non prévu : on arrête la compilation avec un message clair -#}
        {{ exceptions.raise_compiler_error("essayer_cast : type '" ~ type_sql ~ "' non géré (entier, décimal, date)") }}
    {%- endif -%}
{%- endmacro %}



{# ---------- 3a. Dev (DuckDB) : contrôle + TRY_CAST ---------- #}
{# TRY_CAST protège en plus des dates impossibles (2023-02-30 -> NULL) #}
{% macro duckdb__essayer_cast(expression, type_sql) -%}
    CASE
        WHEN TRIM(CAST({{ expression }} AS VARCHAR)) ~ '{{ essayer_cast_motif(type_sql) }}'
        THEN TRY_CAST(TRIM(CAST({{ expression }} AS VARCHAR)) AS {{ type_sql }})
    END
{%- endmacro %}


{# ---------- 3b. Prod (Postgres) et toute autre base : contrôle + CAST ---------- #}
{# Pas de TRY_CAST : le contrôle évite l'erreur pour tout texte mal formé        #}
{% macro default__essayer_cast(expression, type_sql) -%}
    CASE
        WHEN TRIM(CAST({{ expression }} AS VARCHAR)) ~ '{{ essayer_cast_motif(type_sql) }}'
        THEN CAST(TRIM(CAST({{ expression }} AS VARCHAR)) AS {{ type_sql }})
    END
{%- endmacro %}



