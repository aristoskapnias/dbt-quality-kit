{% test unique_combination_full_row(
    model,
    combination_of_columns,
    where=None,
    include_nulls=False,
    quote_columns=False
) %}

{# 
  Works for both models and sources because `model` is a Relation provided by dbt.
  This test flags ALL rows that are part of duplicate combinations and returns FULL RECORDS.
  With +store_failures: true, dbt will persist the full rows in the audit schema.
#}

{% set cols = combination_of_columns %}
{% if quote_columns %}
  {% set cols = cols | map('adapter.quote') | list %}
{% endif %}

with base as (
    select
        *
    from {{ model }}
    {% if where %}
      where {{ where }}
    {% endif %}
),

dupe_keys as (
    select
        {% for col in cols -%}
          {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %},
        count(*) as cnt
    from base
    {%- if not include_nulls %}
      where
      {# exclude any combination where at least one key column is null #}
      {% for col in cols -%}
        {{ col }} is not null{% if not loop.last %} and {% endif %}
      {%- endfor %}
    {%- endif %}
    group by
        {% for col in cols -%}
          {{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    having count(*) > 1
),

failures as (
    select m.*
    from base as m
    join dupe_keys as k
      on
      {% for col in cols -%}
        m.{{ col }} = k.{{ col }}{% if not loop.last %} and {% endif %}
      {%- endfor %}
)

select * from failures

{% endtest %}
