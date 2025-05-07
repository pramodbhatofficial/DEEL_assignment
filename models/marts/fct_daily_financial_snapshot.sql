-- models/marts/fct_daily_financial_snapshot.sql
-- This model creates a daily financial snapshot showing the NET cumulative balance per organization.
-- It includes 'paid' invoices as positive contributions and 'credited'/'refunded' as negative contributions.

{# Step 1: Fetch the global minimum invoice date using run_query #}
{% set get_min_date_query %}
SELECT global_min_invoice_date FROM {{ ref('int_global_min_invoice_date') }} LIMIT 1
{% endset %}

{% set min_date_result = run_query(get_min_date_query) %}

{# Step 2: Set the start_date variable for the date_spine macro #}
{% if execute and min_date_result and min_date_result.rows and min_date_result.rows[0] %}
  {% set start_date_for_spine_value = min_date_result.rows[0][0] %}
{% else %}
  {% set start_date_for_spine_value = '2000-01-01' %} {# Fallback for parsing #}
{% endif %}


WITH stg_invoices_relevant AS (
    -- Step 3: Select relevant invoices and determine their contribution type (inflow/outflow)
    SELECT
        organization_id,
        created_date,
        status,
        CASE 
            WHEN status = 'paid' THEN payment_amount_usd
            WHEN status IN ('credited', 'refunded') THEN amount_usd * -1
            ELSE 0
        END AS daily_amount_change_usd
    FROM {{ ref('stg_invoices') }}
    WHERE status IN ('paid', 'credited', 'refunded')
      AND CASE 
            WHEN status = 'paid' THEN payment_amount_usd IS NOT NULL
            WHEN status IN ('credited', 'refunded') THEN amount_usd IS NOT NULL
          END
),

daily_net_aggregates AS (
    -- Step 4: Aggregate the NET daily change for each organization.
    SELECT
        organization_id,
        created_date AS transaction_date, 
        SUM(daily_amount_change_usd) AS daily_net_change_usd
    FROM stg_invoices_relevant
    GROUP BY
        organization_id,
        created_date
),

organization_first_transaction_date AS (
    -- Step 5: Find the very first date each organization had ANY relevant transaction.
    SELECT
        organization_id,
        MIN(transaction_date) AS first_transaction_date
    FROM daily_net_aggregates
    GROUP BY
        organization_id
),

-- Step 6: Generate the date spine using the macro.
-- Note: This CTE is now just the macro call. We select from it in the next CTE.
_date_spine_source AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ start_date_for_spine_value ~ "', 'YYYY-MM-DD')", -- Explicit TO_DATE with fetched value
        end_date="CURRENT_DATE()" 
    ) }}
),

-- Step 6b: Select and cast the date from the macro output.
date_spine AS (
    SELECT CAST(date_day AS DATE) AS snapshot_date
    FROM _date_spine_source
),

organization_daily_scaffold AS (
    -- Step 7: Create the "scaffold".
    SELECT
        ds.snapshot_date, 
        oftd.organization_id
    FROM date_spine ds
    CROSS JOIN organization_first_transaction_date oftd 
    WHERE 
        ds.snapshot_date >= oftd.first_transaction_date 
        AND ds.snapshot_date <= CURRENT_DATE() 
),

final_dataset AS (
    -- Step 8: Join the actual daily NET changes onto the complete daily scaffold.
    SELECT
        ods.snapshot_date,
        ods.organization_id,
        COALESCE(dna.daily_net_change_usd, 0) AS daily_net_change_usd 
    FROM organization_daily_scaffold ods
    LEFT JOIN daily_net_aggregates dna
        ON ods.organization_id = dna.organization_id
        AND ods.snapshot_date = dna.transaction_date 
)

-- Step 9: Calculate the cumulative NET balance using a window function.
SELECT
    snapshot_date,
    organization_id,
    daily_net_change_usd,
    SUM(daily_net_change_usd) OVER (
        PARTITION BY organization_id 
        ORDER BY snapshot_date ASC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
    ) AS cumulative_net_financial_balance_usd
FROM final_dataset
ORDER BY
    organization_id,
    snapshot_date
