{% macro get_country_from_name() %}

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

{% macro get_country_from_city_tax_code() %}

SELECT
    country_id
    , city_code
    , COUNT(1) as _n_
FROM {{ source('staging', 'cities') }}
GROUP BY 1, 2
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY country_id ORDER BY _n_ DESC) = 1

{% endmacro %}

{% macro get_country_from_country_city_tax_code() %}

SELECT
    id
    , at_code
    , COUNT(1) as _n_
FROM {{ source('staging', 'countries') }}
GROUP BY 1, 2
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY at_code ORDER BY _n_ DESC) = 1

{% endmacro %}

{% macro get_country_from_countries_alias() %}

SELECT
    country_id
    , alias
    , COUNT(1) as _n_
FROM {{ source('staging', 'countries_alias') }}
group by 1, 2
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY alias ORDER BY _n_ DESC) = 1

{% endmacro %}

{% macro get_country_from_city() %}

SELECT
    country_id
    , id
    , COUNT(1) as _n_
FROM {{ source('staging', 'cities') }}
GROUP BY 1, 2
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY country_id ORDER BY _n_ DESC) = 1

{% endmacro %}

