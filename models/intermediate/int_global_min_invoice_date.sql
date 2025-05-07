    -- models/intermediate/int_global_min_invoice_date.sql
    -- This model calculates the earliest 'paid' invoice date across all organizations
    -- to be used as the starting point for the date spine in the financial snapshot.
    -- It uses COALESCE to ensure it always returns a valid date even if no paid invoices exist.

    WITH stg_invoices_paid AS (
        -- Select created_date from paid invoices
        SELECT
            created_date 
        FROM {{ ref('stg_invoices') }} 
        WHERE
            status = 'paid' AND payment_amount_usd IS NOT NULL 
    )

    SELECT
        COALESCE(MIN(created_date), CURRENT_DATE()) AS global_min_invoice_date 
        -- If no paid invoices, defaults to today, date_spine will be for 1 day.
    FROM stg_invoices_paid
 
    