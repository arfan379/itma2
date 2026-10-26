{#-
  Override bawaan dbt supaya nama skema PERSIS sama dengan yang ditulis di
  dbt_project.yml (raw / staging / marts).

  Tanpa macro ini dbt menamai skema "<target.schema>_<custom>", sehingga hasilnya
  menjadi public_staging, public_marts, public_raw -- tidak sesuai dokumentasi
  dan tidak sejajar dengan skema Bronze (raw) yang dibuat infra/init_db.sql.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
