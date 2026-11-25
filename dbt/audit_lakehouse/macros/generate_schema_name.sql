{#- Use the configured schema as-is (audit_silver, audit_gold), not <target>_<schema>:
    namespaces in the catalog are governed names shared with other engines. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
