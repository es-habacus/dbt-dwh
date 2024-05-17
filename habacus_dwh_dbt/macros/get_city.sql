{% macro get_city_from_name() %}

SELECT
    id
    , country_id
    , LOWER(regexp_replace(normalize(name_it, NFD), r"\pM", '')) as name_it
    , LOWER(regexp_replace(normalize(name_en, NFD), r"\pM", '')) as name_en
    , COUNT(1) as _n_
FROM {{ source('staging', 'cities') }}
GROUP BY 1, 2, 3, 4
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY id, country_id ORDER BY _n_ DESC) = 1

{% endmacro %}

{% macro get_city_from_tax_code() %}

SELECT
    id
    , city_code
    , COUNT(1) as _n_
FROM {{ source('staging', 'cities') }}
GROUP BY 1, 2
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _n_ DESC) = 1

{% endmacro %}

{% macro get_city_from_cities_alias() %}

SELECT
    city_id
    , country_id
    , alias
    , COUNT(1) as _n_
FROM {{ source('staging', 'cities_alias') }}
WHERE id != 999999
group by 1, 2, 3
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY alias, country_id ORDER BY _n_ DESC) = 1

{% endmacro %}