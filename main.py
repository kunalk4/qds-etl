import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, Date
from time import time
from datetime import datetime

# sqlalchemy connection strings
url = r"mssql+pyodbc://investment_app:!Vision.solve.winter-12351-$$@svldn048.sarasin.co.uk/investment_app?driver=ODBC" \
      r"+Driver+17+for+SQL+Server"

engine = create_engine(url)

connection = engine.connect()

query = """
SELECT 
    q.idx, 
    q.[1-sided.lower], 
    q.[1-sided.upper], 
    q.id, 
    q.benchmark,
    q.[sub_bm], 
    q.key_date
FROM 
    qds_trends_chart q
INNER JOIN (
    SELECT 
        id, 
        MAX(date) AS latest_date
    FROM 
        qds_trends_chart
    GROUP BY 
        id
) latest ON q.id = latest.id AND q.date = latest.latest_date;
"""
try:
    df = pd.DataFrame(connection.execute(text(query)).fetchall())

finally:
    connection.close()

df1 = df[df['sub_bm'].isna()]
df2 = df[df['sub_bm'].notna()]

df_1 = df1.copy()
df_1['upper'] = df_1.apply(lambda row: 1 if row['idx'] > row['1-sided.upper'] else 0, axis=1)
df_1['lower'] = df_1.apply(lambda row: 1 if row['idx'] < row['1-sided.lower'] else 0, axis=1)

df_2 = df2.copy()
df_2['upper'] = df_2.apply(lambda row: 1 if row['idx'] > row['1-sided.upper'] else 0, axis=1)
df_2['lower'] = df_2.apply(lambda row: 1 if row['idx'] < row['1-sided.lower'] else 0, axis=1)

# Merging the two DataFrames on 'id'
df_merged = pd.merge(df_1, df_2, on='id', suffixes=('_df1', '_df2'))

# Creating the 'value' column based on the specified conditions
df_merged['upper'] = ((df_merged['upper_df1'] == 1) & (df_merged['upper_df2'] == 1)).astype(int)
df_merged['lower'] = ((df_merged['lower_df1'] == 1) & (df_merged['lower_df2'] == 1)).astype(int)

# Keeping only the 'id' and 'value' columns in the final DataFrame
df_merged = df_merged[['id', 'upper', 'lower']]

connection = engine.connect()

query = """
SELECT *
from qds_screen
"""
try:
    df = pd.DataFrame(connection.execute(text(query)).fetchall())

finally:
    connection.close()

df_merged = df_merged.rename(columns={'id': 'isin'})

# joining the upper and lower columns into df
df_new = pd.merge(df, df_merged[['isin', 'upper', 'lower']], on='isin', how='left')

# convert pandas timestamp columns to Datetime to prevent >1 SQL timestamp column error
dtype = {
    "key_date": Date,
}

# df -> sql db
df_new.to_sql('qds_screen_new', engine, dtype=dtype, if_exists='replace', index=False)

print('Successfully submitted script to SQL Server DB')