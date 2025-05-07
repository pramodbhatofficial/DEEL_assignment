        -- Checks if amount_usd is NULL for credited or refunded invoices, 
        -- which would prevent correct net balance calculation.
        -- Expects 0 rows.
        SELECT
            invoice_id,
            organization_id,
            status,
            amount_usd
        FROM {{ ref('stg_invoices') }}
        WHERE 
            status IN ('credited', 'refunded')
            AND amount_usd IS NULL
        