    -- models/staging/stg_organizations.sql

    WITH source_data AS (
        SELECT *
        FROM {{ source('src_public', 'organizations') }}
    )

    SELECT
        "ORGANIZATION_ID" AS organization_id,
        "FIRST_PAYMENT_DATE" AS first_payment_date,
        "LAST_PAYMENT_DATE" AS last_payment_date,
        "LEGAL_ENTITY_COUNTRY_CODE" AS legal_entity_country_code,
        "COUNT_TOTAL_CONTRACTS_ACTIVE" AS count_total_contracts_active,
        "CREATED_DATE" AS created_date_timestamp 
        CAST("CREATED_DATE" AS DATE) AS organization_created_date 
    FROM source_data
    