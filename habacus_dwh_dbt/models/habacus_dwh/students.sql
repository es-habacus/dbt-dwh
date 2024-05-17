SELECT
    id
    , lead_source
    , SHA256(tax_code)AS hash_tax_code
    , SHA256(email) AS hash_email
    , EXTRACT(YEAR FROM born_at) AS born_year
    , CASE WHEN SAFE_CAST(SUBSTR(tax_code, 9, 2) AS INT64) > 40 THEN 'F' 
        WHEN SAFE_CAST(SUBSTR(tax_code, 9, 2) AS INT64) <= 40 THEN 'M'
        ELSE 'X' END AS gender
    , created_at
    , updated_at
FROM {{ source('staging', 'students') }}