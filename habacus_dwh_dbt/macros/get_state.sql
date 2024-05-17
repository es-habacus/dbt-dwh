{% macro get_state_from_name() %}

SELECT
    id
    , LOWER(regexp_replace(normalize(name_it, NFD), r"\pM", '')) as name_it
    , LOWER(regexp_replace(normalize(name_en, NFD), r"\pM", '')) as name_en
    , COUNT(1) as _n_
FROM {{ source('staging', 'countries') }}
GROUP BY 1, 2, 3
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _n_ DESC) = 1

{% endmacro %}

{% macro get_state_from_city() %}

SELECT
    state_id
    , id
    , COUNT(1) as _n_
FROM {{ source('staging', 'cities') }}
GROUP BY 1, 2
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _n_ DESC) = 1

{% endmacro %}

{% macro get_state_from_school_name() %}

SELECT
    state_id
    , id
    , COUNT(1) as _n_
FROM {{ source('staging', 'cities') }}
GROUP BY 1, 2
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY state_id ORDER BY _n_ DESC) = 1

{% endmacro %}