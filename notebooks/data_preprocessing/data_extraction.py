import wrds
import pandas as pd
from bond_utils import get_bonds


db = wrds.Connection(wrds_username='jb101')


start_date = '2014-01-01'
end_date = '2016-12-31'
num_bonds = 500



def get_data(start_date, end_date, num_bonds):
  
  liquid_cusips = get_bonds(start_date, end_date, num_bonds)
  print("Starting data extraction...")

  query = """

  WITH base AS (
    SELECT *
    FROM trace.trace_enhanced
    WHERE trd_exctn_dt BETWEEN DATE %s AND DATE %s
      AND cusip_id = ANY(%s)
      AND rptd_pr IS NOT NULL
      AND entrd_vol_qt > 0
  ),

  -- Identify all correction/cancel/reversal/error reports
  error_reports AS (
    SELECT *
    FROM base
    WHERE trc_st IN ('C','W','R','X','Y')
      OR asof_cd = 'R'
  ),

  -- Identify the original "T" trades that match these error reports by 7-key linkage
  originals_to_drop_strict AS (
    SELECT DISTINCT t.*
    FROM base t
    JOIN error_reports e
      ON t.cusip_id     = e.cusip_id
    AND t.trd_exctn_dt = e.trd_exctn_dt
    AND t.trd_exctn_tm = e.trd_exctn_tm
    AND t.rptd_pr      = e.rptd_pr
    AND t.entrd_vol_qt = e.entrd_vol_qt
    AND t.rpt_side_cd  = e.rpt_side_cd
    AND t.cntra_mp_id   = e.cntra_mp_id
    AND t.msg_seq_nb   = e.msg_seq_nb
    WHERE t.trc_st = 'T'
  ),


  originals_to_drop_fallback AS (
    SELECT DISTINCT ON (t.cusip_id, t.trd_exctn_dt, t.rptd_pr, 
                        t.entrd_vol_qt, t.rpt_side_cd, t.cntra_mp_id
                        )
                        t.*
    FROM base t
    JOIN error_reports e
      ON e.trc_st = 'R'
    AND t.cusip_id     = e.cusip_id
    AND t.trd_exctn_dt = e.trd_exctn_dt
    AND t.rptd_pr      = e.rptd_pr
    AND t.entrd_vol_qt = e.entrd_vol_qt
    AND t.rpt_side_cd  = e.rpt_side_cd
    AND t.cntra_mp_id   = e.cntra_mp_id
    WHERE t.trc_st = 'T'
    ORDER BY 
      t.cusip_id, t.trd_exctn_dt, t.rptd_pr, t.entrd_vol_qt, t.rpt_side_cd, t.cntra_mp_id, t.trd_exctn_tm ASC
  ), 

  originals_to_drop AS (
    SELECT * FROM originals_to_drop_strict
    UNION
    SELECT * FROM originals_to_drop_fallback
  ),

  -- Union the two lists to drop (error reports + their originals)
  to_remove AS (
    SELECT cusip_id, trd_exctn_dt, trd_exctn_tm,
          rptd_pr, entrd_vol_qt, rpt_side_cd, cntra_mp_id, msg_seq_nb
    FROM error_reports
    UNION
    SELECT cusip_id, trd_exctn_dt, trd_exctn_tm,
          rptd_pr, entrd_vol_qt, rpt_side_cd, cntra_mp_id, msg_seq_nb
    FROM originals_to_drop
  ),

  -- Keep only clean "T" trades
  clean AS (
    SELECT b.*
    FROM base b
    LEFT JOIN to_remove r
      ON b.cusip_id     = r.cusip_id
    AND b.trd_exctn_dt = r.trd_exctn_dt
    AND b.trd_exctn_tm = r.trd_exctn_tm
    AND b.rptd_pr      = r.rptd_pr
    AND b.entrd_vol_qt = r.entrd_vol_qt
    AND b.rpt_side_cd  = r.rpt_side_cd
    AND b.cntra_mp_id   = r.cntra_mp_id
    AND b.msg_seq_nb   = r.msg_seq_nb
    WHERE r.cusip_id IS NULL AND b.trc_st = 'T'
  ),

  -- Filter hours, price, etc.
  step_hours AS (
    SELECT *
    FROM clean
    WHERE CAST(trd_exctn_tm AS TIME) BETWEEN TIME '08:00:00' AND TIME '17:15:00'
  ),
  step_price AS (
    SELECT *
    FROM step_hours
    WHERE rptd_pr >= 10
  ),
  step_subprd AS (
    SELECT *
    FROM step_price
    WHERE sub_prdct = 'CORP'
  ),
  step_sale AS (
    SELECT *
    FROM step_subprd
    WHERE COALESCE(sale_cndtn_cd,'') NOT IN ('W','L','T','S','P')
  ),


  step_d2c AS (
    SELECT *
    FROM step_sale
    WHERE (buy_cpcty_cd IS NOT NULL OR sell_cpcty_cd IS NOT NULL)
  ),



  step_capacity AS (
    SELECT *
    FROM step_d2c
    WHERE NOT (
      (buy_cpcty_cd IS DISTINCT FROM sell_cpcty_cd)
      AND buy_cpcty_cd IS NOT NULL
      AND sell_cpcty_cd IS NOT NULL
    )
  ),

  step_agency AS (
    SELECT *
    FROM step_capacity
    WHERE NOT (
          COALESCE(buy_cpcty_cd, sell_cpcty_cd) = 'A'
        )
  ),



  -- Holiday filter using CRSP trading dates
  valid_days AS (
    SELECT DISTINCT "date"::date AS trd_exctn_dt
    FROM crsp.dsi
    WHERE "date" BETWEEN DATE '2015-01-01' AND DATE '2016-12-31'
  ),

  step_biz AS (
    SELECT s.*
    FROM step_agency AS s
    JOIN valid_days AS v
      ON s.trd_exctn_dt::date = v.trd_exctn_dt
    WHERE EXTRACT(ISODOW FROM s.trd_exctn_dt) < 6
  )

  SELECT
    s.cusip_id,
    s.trd_exctn_dt,
    s.trd_exctn_tm,
    s.msg_seq_nb,
    s.rptd_pr,
    s.entrd_vol_qt,
    s.rpt_side_cd,
    COALESCE(s.buy_cpcty_cd, s.sell_cpcty_cd) AS capacity,
    s.trc_st,
    s.sale_cndtn_cd,
    s.sub_prdct,
    s.cntra_mp_id
  FROM step_biz AS s
  ORDER BY s.cusip_id, s.trd_exctn_dt, s.trd_exctn_tm, s.msg_seq_nb
  LIMIT 8000000;




  """


  df = db.raw_sql(query, params=[(start_date, end_date, liquid_cusips)])
  print("Data obtained successfully")
  print("--------------------------------")
  print(f"Number of observations: {len(df)}")
  print("--------------------------------")
  df.to_csv('C:/Users/wor_j/ML/School_projects/adv_ml_bond_project/price_impact/notebooks/data/processed_bond_trades_sample.csv', index=False)

  return df


get_data(start_date, end_date, num_bonds)