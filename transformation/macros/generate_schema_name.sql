{#
  generate_schema_name — choisit le schéma où dbt crée chaque modèle.
  Par défaut, dbt fait : <schéma du target>_<+schema>  (ex. silver_gold)
  Ici : si un +schema est donné (silver, gold), on l'utilise TEL QUEL ;
        sinon on prend le schéma du target (profiles.yml).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}