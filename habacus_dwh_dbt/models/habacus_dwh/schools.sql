WITH 
-- First attempt to match country_id
country_from_name AS
    (
        {{ get_country_from_name() }}
    ),
-- Second attempt to match country_id
country_from_countries_alias AS
    (
        {{ get_country_from_countries_alias() }}
    ),
-- First attempt to match city_id
city_from_name AS
    (
        {{ get_city_from_name() }}
    ),
-- Second attempt to match city_id
city_from_cities_alias AS
    (
        {{ get_city_from_cities_alias() }}
    ),
-- third attempt to match city_id
city_from_tax_code AS
    (
        {{ get_city_from_tax_code() }}
    ),
-- First attempt to match stated_id
state_from_name AS
    (
        {{ get_state_from_name() }}
    ),
state_from_city AS
    (
        {{ get_state_from_city() }}
    ),
-- First attempt to match province_id
province_from_name AS
    (
        {{ get_province_from_name() }}
    ),
-- Second attempt to match province_id
province_from_city AS
    (
        {{ get_province_from_city() }}
    ),
-- Third attempt to match country_id
country_from_city AS
    (
        {{ get_country_from_city() }}
    ),
tmp_table_1 AS 
    (
    SELECT
        s.id 
        , s.external_id
        , s.external_identifier
        , s.external_system
        , s.name
        , COALESCE(s.business_name, s.name) AS business_name
        , s.phone
        , s.country
        , COALESCE(cfn.id, cfca.country_id) as country_id
        , s.website_url
        , s.address
        , s.zip
        , s.email
        , s.is_online
        , s.is_italian
        -- , s.max_annual_fee
        , s.max_annual_fee_currency
        , s.verified
        , s.created_at
        , s.updated_at
        , city
        , state
        , sfn.id AS state_id
        , province
    FROM {{ source('staging', 'schools') }} AS s
    LEFT JOIN country_from_name AS cfn
        ON (LOWER(s.country) = cfn.name_it or LOWER(s.country) = cfn.name_en)
    LEFT JOIN country_from_countries_alias AS cfca
        ON LOWER(s.country) = lower(cfca.alias)
    LEFT JOIN state_from_name AS sfn 
        ON (LOWER(s.state) = sfn.name_it or LOWER(s.state) = sfn.name_en)
    ),
tmp_table_2 AS 
    (
    SELECT
        tt1.*
        , COALESCE(c2fn.id, c2fca.city_id) AS city_id
    FROM tmp_table_1 as tt1
    LEFT JOIN city_from_name AS c2fn
        ON (LOWER(tt1.city) = c2fn.name_it or LOWER(tt1.city) = c2fn.name_en)
    LEFT JOIN city_from_cities_alias as c2fca
        ON LOWER(tt1.city) = LOWER(c2fca.alias) and tt1.country_id = c2fca.country_id
    ),
tmp_table_3 AS
    (
    SELECT
        tt2.*
        , COALESCE(tt2.state_id, sfc.state_id) AS state_id_step_2
    FROM tmp_table_2 AS tt2 
    LEFT JOIN state_from_city as sfc 
        ON tt2.city_id = sfc.id
    )
SELECT
    *
FROM tmp_table_3;


