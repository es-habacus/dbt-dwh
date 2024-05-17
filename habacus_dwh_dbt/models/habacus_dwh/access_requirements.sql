SELECT
    *
FROM {{ source('staging', 'access_requirements') }}