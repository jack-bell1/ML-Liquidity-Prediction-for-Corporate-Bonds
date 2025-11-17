import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns





df = pd.read_csv('C:/Users/wor_j/ML/School_projects/adv_ml_bond_project/price_impact/notebooks/data/processed_bond_trades_sample.csv')






###############################################
# SECTION 1: RPT Declaration
###############################################
print("--------------------------------")
print("Declaring RPTs...")


# Ensure correct order
df = df.sort_values(['cusip_id','trd_exctn_dt','trd_exctn_tm','msg_seq_nb']).reset_index(drop=True)
df['entrd_vol_qt'] = pd.to_numeric(df['entrd_vol_qt'], errors='coerce')

# Group by same bond, date, size, dealer
df['grp'] = df.groupby(['cusip_id','trd_exctn_dt','entrd_vol_qt']).ngroup()

# Shift to see next and previous trade
for col in ['grp','rpt_side_cd']:
    df[f'next_{col}'] = df[col].shift(-1)
    df[f'prev_{col}'] = df[col].shift(1)

# Forward pair (B,S) or (S,B) within same size-run and dealer
cond_fwd = (
    (df['grp'] == df['next_grp']) &
    (df['rpt_side_cd'].isin(['B','S'])) &
    (df['next_rpt_side_cd'].isin(['B','S'])) &
    (df['rpt_side_cd'] != df['next_rpt_side_cd'])
)

# Backward check for the same reason
cond_back = (
    (df['grp'] == df['prev_grp']) &
    (df['rpt_side_cd'].isin(['B','S'])) &
    (df['prev_rpt_side_cd'].isin(['B','S'])) &
    (df['rpt_side_cd'] != df['prev_rpt_side_cd'])
)

df['is_rpt'] = cond_fwd | cond_back

# Remove overlapping patterns like B–S–B
df.loc[
    df['is_rpt'] &
    df['is_rpt'].shift(1, fill_value=False) &
    (df['grp'] == df['grp'].shift(1)),
    'is_rpt'
] = False

# Drop RPTs, assign epsilon
df_rpt_clean = df.loc[~df['is_rpt']].copy()
df_rpt_clean['epsilon'] = df_rpt_clean['rpt_side_cd'].map({'S': +1, 'B': -1}).fillna(0).astype('int8')

print(f"Flagged {df['is_rpt'].sum():,} potential RPTs "
      f"({df['is_rpt'].mean():.1%} of all trades)")


print("RPTs declared successfully")
print("--------------------------------")

###############################################
# SECTION 2: 
###############################################


print("Calculating spread...")


# assume df_rpt_clean from before (with epsilon)
df_spread = df_rpt_clean.copy()

# ensure datetime format
df_spread['datetime'] = pd.to_datetime(
    df_spread['trd_exctn_dt'].astype(str) + ' ' + df_spread['trd_exctn_tm'].astype(str)
)
df_spread = df_spread.sort_values(['cusip_id', 'datetime']).reset_index(drop=True)

# compute lag values within same cusip_id
for col in ['rptd_pr', 'epsilon', 'datetime']:
    df_spread[f'prev_{col}'] = df_spread.groupby('cusip_id')[col].shift(1)

# time difference in minutes
df_spread['dt_min'] = (df_spread['datetime'] - df_spread['prev_datetime']).dt.total_seconds() / 60

# identify valid pairs: same bond, opposite signs, within 5 minutes
mask_valid = (
    (df_spread['epsilon'] * df_spread['prev_epsilon'] == -1)
    & (df_spread['dt_min'] > 0)
    & (df_spread['dt_min'] <= 5)
)

# compute midprice and effective spread (bps)
df_spread.loc[mask_valid, 'midprice'] = (
    df_spread.loc[mask_valid, 'rptd_pr'] + df_spread.loc[mask_valid, 'prev_rptd_pr']
) / 2

df_spread.loc[mask_valid, 'spread_bps'] = (
    10000 * np.abs(df_spread.loc[mask_valid, 'rptd_pr'] - df_spread.loc[mask_valid, 'prev_rptd_pr'])
    / df_spread.loc[mask_valid, 'midprice']
)

# keep only valid pairs
df_spread_pairs = df_spread.loc[mask_valid, [
    'cusip_id','trd_exctn_dt','datetime','midprice','spread_bps'
]].copy()

# daily average per CUSIP
daily_spread = (
    df_spread_pairs
    .groupby(['cusip_id','trd_exctn_dt'])
    ['spread_bps']
    .mean()
    .reset_index()
    .rename(columns={'spread_bps':'avg_spread_bps'})
)

print(f"Valid pairs: {len(df_spread_pairs):,} ({len(df_spread_pairs)/len(df_rpt_clean):.1%} of total)")


print("Spread calculated successfully")
print("--------------------------------")



print("Spread calculated successfully")
print("--------------------------------")

print("Exporting daily spreads...")
daily_spread = daily_spread.sort_values(["trd_exctn_dt", "cusip_id"])
daily_spread.to_csv("C:/Users/wor_j/ML/School_projects/adv_ml_bond_project/price_impact/notebooks/data/daily_spread.csv", index=False)
print("Daily spreads exported successfully")
print("--------------------------------")













###############################################
# SECTION 3: Weekly Aggregation
###############################################

print("Aggregating weekly spreads...")

# ensure date type
daily_spread['trd_exctn_dt'] = pd.to_datetime(daily_spread['trd_exctn_dt'])

# compute normalized week_start
daily_spread['week_start'] = (
    daily_spread['trd_exctn_dt'] - 
    pd.to_timedelta(daily_spread['trd_exctn_dt'].dt.weekday, unit='D')
).dt.normalize()

weekly_spread = (
    daily_spread
    .groupby(['cusip_id', 'week_start'])
    ['avg_spread_bps']
    .mean()
    .reset_index()
)
weekly_spread.rename(columns={'avg_spread_bps':'weekly_avg_spread_bps'}, inplace=True)




#########################################################
# SECTION 4: Count valid buy–sell pairs per week
#########################################################
print("Counting valid buy–sell pairs per week...")
df_pairs = df_spread_pairs.copy()

df_pairs['week_start'] = (
    df_pairs['datetime'] - 
    pd.to_timedelta(df_pairs['datetime'].dt.weekday, unit='D')
).dt.normalize()

weekly_counts = (
    df_pairs.groupby(['cusip_id','week_start'])
            .size()
            .reset_index(name='n_pairs')
)

weekly_spread = weekly_spread.merge(
    weekly_counts,
    on=['cusip_id','week_start'],
    how='left'
)

weekly_spread['n_pairs'] = weekly_spread['n_pairs'].fillna(0).astype(int)


# Fill missing with 0 (these are weeks that had daily spreads via averaging but no pairs)
weekly_spread['n_pairs'] = weekly_spread['n_pairs'].fillna(0).astype(int)


print(f"Valid pairs: {len(df_spread_pairs):,} ({len(df_spread_pairs)/len(df_rpt_clean):.1%} of total)")
print(f"Weekly rows: {len(weekly_spread):,} from {daily_spread['trd_exctn_dt'].nunique()} trading days.")


print("Weekly spreads aggregated successfully")
print("--------------------------------")

###############################################
# SECTION 4: Export
###############################################

print("Exporting data...")

weekly_spread = weekly_spread.sort_values(["week_start", "cusip_id"]).reset_index(drop=True)



weekly_spread.to_csv('C:/Users/wor_j/ML/School_projects/adv_ml_bond_project/price_impact/notebooks/data/weekly_spread.csv', index=False)

print("Data exported successfully")
print("--------------------------------")




print("Calculating number of weeks with 0 spread...")


count = 0


for i in range(len(weekly_spread)):
    if weekly_spread['weekly_avg_spread_bps'].iloc[i] == weekly_spread['weekly_avg_spread_bps'].min():
        count += 1

print(f"Number of weeks with 0 spread: {count} ({count / len(weekly_spread):.1%} of total)")
print("--------------------------------")

print('Plotting distribution of spreads...')



print(weekly_spread['weekly_avg_spread_bps'].describe())
sns.histplot(weekly_spread['weekly_avg_spread_bps'], bins=300)
plt.title('Distribution of Weekly Spreads')
plt.xlabel('Weekly Spread (bps)')
plt.ylabel('Frequency')
plt.show()




print("Distribution plotted successfully")
print("--------------------------------")







