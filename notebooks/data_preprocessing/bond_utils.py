import wrds
import pandas as pd

db = wrds.Connection(wrds_username='jb101')



def get_bonds(start_date, end_date, num_bonds, db=db):
    print("Getting bonds...")
    query = """
        WITH universe AS (
            SELECT
                cusip,
                SUM(CASE WHEN t_volume > 0 THEN 1 ELSE 0 END) AS active_months,
                COUNT(*) AS total_months,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY t_volume) AS median_volume,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY gap) AS median_gap,
                SUM(CASE WHEN t_spread IS NOT NULL THEN 1 ELSE 0 END) AS valid_spread_months
            FROM wrdsapps_bondret.bondret
            WHERE date BETWEEN %s AND %s
            GROUP BY cusip
        )
        SELECT cusip
        FROM universe
        WHERE active_months >= 12
          AND median_volume > 0
          AND median_gap <= 10
          AND valid_spread_months >= 6
        ORDER BY median_volume DESC
        LIMIT %s
    """
    df = db.raw_sql(query, params=(start_date, end_date, num_bonds))  
    print("Bonds obtained successfully")
    print("--------------------------------")
    print(f"Number of bonds: {len(df)}")
    print("--------------------------------")
    return df['cusip'].tolist()



