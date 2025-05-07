        -- Checks if payment_amount_usd is NULL for paid invoices,
        -- which would prevent correct net balance calculation.
        -- Expects 0 rows.
        SELECT
            invoice_id,
            organization_id,
            status,
            payment_amount_usd
        FROM {{ ref('stg_invoices') }}
        WHERE 
            status = 'paid'
            AND payment_amount_usd IS NULL
        