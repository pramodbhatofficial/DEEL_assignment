    -- models/marts/dim_organizations.sql

    WITH stg_orgs AS (
        SELECT *
        FROM {{ ref('stg_organizations') }}
    )

    SELECT
        organization_id,
        first_payment_date,
        last_payment_date,
        legal_entity_country_code, 
        count_total_contracts_active,
        created_date AS organization_created_date 
    FROM stg_orgs
    