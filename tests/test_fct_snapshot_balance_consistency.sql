        -- Checks if the cumulative balance ever decreases when the daily change is non-negative.
        -- This tests the integrity of the window function and date spine.
        -- Expects 0 rows.
        WITH daily_snapshot AS (
            SELECT * FROM {{ ref('fct_daily_financial_snapshot') }}
        ),

        lagged_snapshot AS (
            SELECT
                *,
                LAG(cumulative_net_financial_balance_usd, 1, 0) OVER (PARTITION BY organization_id ORDER BY snapshot_date) AS previous_day_balance
            FROM daily_snapshot
        )

        SELECT
            snapshot_date,
            organization_id,
            daily_net_change_usd,
            previous_day_balance,
            cumulative_net_financial_balance_usd
        FROM lagged_snapshot
        WHERE 
            daily_net_change_usd >= 0 -- Check only on days where balance shouldn't decrease
            AND cumulative_net_financial_balance_usd < previous_day_balance -- Balance decreased unexpectedly
        