import os
import snowflake.connector
from decimal import Decimal, InvalidOperation
import datetime


SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT') 
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE') 
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')   

# Alerting threshold
ALERT_THRESHOLD_PERCENT = Decimal('50.0')

# --- Helper Function ---
def calculate_percentage_change(old_value, new_value):
    """
    Calculates the percentage change between two values.
    Handles potential None inputs and division by zero.
    Uses Decimal for precision.
    """
    if old_value is None or new_value is None:
        # Cannot calculate change if either value is missing
        # Could happen if an org appears for the first time today
        return None 
    
    # Convert to Decimal for accurate arithmetic
    try:
        # Handle potential non-numeric types if data quality issues exist
        old = Decimal(old_value)
        new = Decimal(new_value)
    except (InvalidOperation, TypeError) as e:
        print(f"  Warning: Could not convert balance values to Decimal: old='{old_value}', new='{new_value}'. Error: {e}")
        return None

    # Handle division by zero
    if old == Decimal('0'):
        if new == Decimal('0'):
            # If both are zero, the change is 0%
            return Decimal('0.0')
        else:
            # If old is zero and new is non-zero, change is technically infinite.
            # For alerting, we can treat this as exceeding the threshold if new > 0.
            # Or return a special value/None. Let's return a large positive/negative number
            # to ensure it triggers the alert if the new balance is significant.
            return Decimal('999999.99') if new > 0 else Decimal('-999999.99')

    # Calculate percentage change: ((new - old) / |old|) * 100
    try:
        change = ((new - old) / abs(old)) * Decimal('100.0')
        return change
    except Exception as e:
        # Catch any other unexpected arithmetic errors
        print(f"  Error calculating percentage change for old='{old}', new='{new}': {e}")
        return None

# --- Main Alerting Logic ---
def check_balance_changes():
    """
    Connects to Snowflake, queries the daily snapshot table for the latest day's changes,
    and prints alerts to the console for significant balance fluctuations.
    """
    conn = None # Initialize connection variable
    try:
        # --- Pre-computation: Get the most recent snapshot date ---
        print("Determining the most recent snapshot date...")
        # Check for required environment variables first
        required_vars = [
            'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'
        ]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            print(f"Error: Missing required Snowflake environment variables: {', '.join(missing_vars)}")
            print("Please set all required SNOWFLAKE_* variables.")
            return

        # Establish connection to get the max date
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cur_date = conn.cursor()
        
        # Find the most recent date present in the snapshot table
        # This handles cases where the table might not yet have data for CURRENT_DATE()
        max_date_query = f"SELECT MAX(snapshot_date) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.fct_daily_financial_snapshot"
        cur_date.execute(max_date_query)
        max_date_result = cur_date.fetchone()
        
        if max_date_result is None or max_date_result[0] is None:
             print("Error: Could not find any data in the fct_daily_financial_snapshot table.")
             return
             
        latest_snapshot_date = max_date_result[0]
        # Ensure it's a date object if it's not already
        if isinstance(latest_snapshot_date, datetime.datetime):
             latest_snapshot_date = latest_snapshot_date.date()
             
        print(f"Most recent snapshot date found: {latest_snapshot_date}")
        
        # Close the date cursor and connection temporarily if needed, or reuse connection
        cur_date.close()
        # Re-establish or reuse connection for the main query
        # For simplicity, we reuse the existing 'conn' object

        # --- Main Query: Get balances for the latest date and the day before ---
        print(f"Querying balances for {latest_snapshot_date} and the previous day...")
        cur_main = conn.cursor()

        # Query uses LAG to get the previous day's balance directly.
        # Filters for the latest snapshot date found.
        query = f"""
        WITH daily_balances AS (
            SELECT
                snapshot_date,
                organization_id,
                cumulative_net_financial_balance_usd,
                LAG(cumulative_net_financial_balance_usd, 1) OVER (
                    PARTITION BY organization_id 
                    ORDER BY snapshot_date ASC
                ) AS previous_day_balance
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.fct_daily_financial_snapshot
            -- Only need data for the latest date and the one before it for comparison
            WHERE snapshot_date >= DATEADD(day, -1, '{latest_snapshot_date}') 
              AND snapshot_date <= '{latest_snapshot_date}'
        )
        SELECT
            snapshot_date,
            organization_id,
            cumulative_net_financial_balance_usd AS current_balance,
            previous_day_balance
        FROM daily_balances
        WHERE snapshot_date = '{latest_snapshot_date}' -- Only process rows for the most recent snapshot date
          -- AND previous_day_balance IS NOT NULL -- Optional: Exclude orgs appearing for the first time today
        ORDER BY organization_id;
        """

        cur_main.execute(query)
        results = cur_main.fetchall()
        print(f"Processing {len(results)} organizations for snapshot date {latest_snapshot_date}.")

        alert_count = 0
        # --- Process results and check for alerts ---
        for row in results:
            snapshot_date, org_id, current_balance, previous_balance = row
            
            # Calculate the percentage change using the helper function
            percentage_change = calculate_percentage_change(previous_balance, current_balance)

            # Check if the change calculation was successful and exceeds the threshold
            if percentage_change is not None:
                if abs(percentage_change) > ALERT_THRESHOLD_PERCENT:
                    alert_count += 1
                    print("-" * 30)
                    print(f"ALERT! Organization ID: {org_id}")
                    print(f"  Date: {snapshot_date}")
                    # Handle None for previous balance display (e.g., first day)
                    prev_display = f"{previous_balance:,.2f}" if previous_balance is not None else "N/A (First Day?)"
                    curr_display = f"{current_balance:,.2f}" if current_balance is not None else "N/A"
                    change_display = f"{percentage_change:+.2f}%" if percentage_change != Decimal('999999.99') and percentage_change != Decimal('-999999.99') else "N/A (From Zero)"

                    print(f"  Previous Balance (USD): {prev_display}")
                    print(f"  Current Balance (USD):  {curr_display}")
                    print(f"  Change: {change_display}")
                    print("-" * 30)

        print(f"\nFinished checking balances. Generated {alert_count} alerts for {latest_snapshot_date}.")

    except snowflake.connector.Error as e:
        # Handle Snowflake specific errors
        print(f"Snowflake Error: {e}")
        print(f"  Error Code: {e.errno}")
        print(f"  SQL State: {e.sqlstate}")
    except Exception as e:
        # Handle other potential errors (e.g., network issues, programming errors)
        print(f"An unexpected error occurred: {e}")
    finally:
        # Ensure the database connection is always closed
        if conn:
            conn.close()
            print("Snowflake connection closed.")

# --- Run the check when the script is executed ---
if __name__ == "__main__":
    check_balance_changes()
