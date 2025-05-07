-- models/staging/stg_invoices.sql

WITH source_data AS (
    SELECT *
    FROM {{ source('src_public', 'invoices') }}
)

SELECT
    "INVOICE_ID" AS invoice_id,
    "PARENT_INVOICE_ID" AS parent_invoice_id,
    "TRANSACTION_ID" AS transaction_id,
    "ORGANIZATION_ID" AS organization_id,
    "TYPE" AS type,
    "STATUS" AS status,
    "CURRENCY" AS currency,
    "PAYMENT_CURRENCY" AS payment_currency,
    "PAYMENT_METHOD" AS payment_method,
    
    TRY_CAST("AMOUNT" AS DECIMAL(38, 2)) AS amount,
    TRY_CAST("PAYMENT_AMOUNT" AS DECIMAL(38, 2)) AS payment_amount,
    TRY_CAST("FX_RATE" AS DECIMAL(13, 8)) AS fx_rate,
    TRY_CAST("FX_RATE_PAYMENT" AS DECIMAL(13, 8)) AS fx_rate_payment,
    
    "CREATED_AT" AS created_at_timestamp,
    CAST("CREATED_AT" AS DATE) AS created_date,

    -- USD equivalent of the primary invoice AMOUNT
    -- Uses FX_RATE (LocalCurrencyUnits per 1 USD)
    CASE
        WHEN "CURRENCY" = 'USD' THEN TRY_CAST("AMOUNT" AS DECIMAL(38, 2))
        WHEN "CURRENCY" != 'USD' AND TRY_CAST("FX_RATE" AS DECIMAL(13, 8)) IS NOT NULL AND TRY_CAST("FX_RATE" AS DECIMAL(13, 8)) != 0 THEN 
            TRY_CAST("AMOUNT" AS DECIMAL(38, 2)) / TRY_CAST("FX_RATE" AS DECIMAL(13, 8))
        ELSE NULL 
    END AS amount_usd,

    -- USD equivalent of the PAYMENT_AMOUNT
    -- Uses FX_RATE_PAYMENT (LocalPaymentCurrencyUnits per 1 USD)
    CASE
        WHEN "PAYMENT_CURRENCY" = 'USD' THEN TRY_CAST("PAYMENT_AMOUNT" AS DECIMAL(38, 2))
        WHEN "PAYMENT_CURRENCY" != 'USD' AND TRY_CAST("FX_RATE_PAYMENT" AS DECIMAL(13, 8)) IS NOT NULL AND TRY_CAST("FX_RATE_PAYMENT" AS DECIMAL(13, 8)) != 0 THEN 
            TRY_CAST("PAYMENT_AMOUNT" AS DECIMAL(38, 2)) / TRY_CAST("FX_RATE_PAYMENT" AS DECIMAL(13, 8))
        ELSE NULL 
    END AS payment_amount_usd
    
FROM source_data
