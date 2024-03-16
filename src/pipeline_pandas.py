import logging
import os
import datetime 
import sqlite3

import pandas as pd

def import_data(db_path,parquet_path):
   cnx = sqlite3.connect(db_path)
   grocery_sales = pd.read_sql_query('SELECT * from grocery_sales',cnx)
   grocery_sales['Date'] = pd.to_datetime(grocery_sales['Date'],errors='coerce')
   grocery_sales.drop(grocery_sales[grocery_sales['Weekly_Sales'] == 'null'].index,axis = 0,inplace=True)
   grocery_sales['Weekly_Sales'] = grocery_sales['Weekly_Sales'].astype(float)
   return grocery_sales, pd.read_parquet(parquet_path)

def extract(df1,df2):
    merged_df = pd.merge(df1,df2,on='index')
    return merged_df

def transform(df):
    df_filled = df.fillna(method='ffill')
    df_filled['Month'] = df_filled['Date'].dt.month
    df_filtered = df_filled[df_filled.Weekly_Sales > 10000]
    clean_data = df_filtered[['Store_ID','Month','Dept','IsHoliday','Weekly_Sales','CPI','Unemployment']]
    return clean_data

def avg_monthly_sales(df):
     return df.groupby('Month').agg(Avg_Sales=('Weekly_Sales','mean')).round(2).reset_index()
     
def load(df1, df2, path1, path2):
    df1.to_csv(path1)
    df2.to_csv(path2)

def validation(path1,path2):
    if os.path.exists(path1) and os.path.exists(path2):
        return True
    else:
        return False

data_dir = '../data/'    
parquet_path = data_dir + 'extra_data.parquet'
clean_data_path = data_dir + 'clean_data.csv'
agg_data_path = data_dir + 'agg_data.csv'
db_path = data_dir + 'walmart.db'

grocery_sales, extra_data = import_data(db_path, parquet_path)
merged_df = extract(grocery_sales, extra_data)
clean_data = transform(merged_df)
agg_data = avg_monthly_sales(clean_data)
load(clean_data, agg_data, clean_data_path, agg_data_path)
validation(clean_data_path, agg_data_path)