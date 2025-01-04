from pyflink.table import (
    EnvironmentSettings, TableEnvironment, DataTypes, Table
)
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble
import pandas as pd
import sqlite3
import os

"""
Flink ETL Pipeline for Walmart Sales Data Analysis

This script implements the same ETL pipeline, but using Apache Flink.
"""

def create_flink_env():
    # Create Flink Table environment
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)
    return t_env

def import_data(t_env, db_path, parquet_path):
    # Load data from SQLite
    conn = sqlite3.connect(db_path)
    sales_df = pd.read_sql_query("SELECT * FROM grocery_sales", conn)
    conn.close()
    
    # Load extra data from Parquet
    extra_df = pd.read_parquet(parquet_path)
    
    # Select only the columns we need from extra_df
    extra_df = extra_df[['index', 'CPI', 'Unemployment']]
    
    # Clean and convert both DataFrames
    sales_df = sales_df.replace({'null': None})
    extra_df = extra_df.replace({'null': None})
    
    # Convert to Flink tables
    grocery_sales = t_env.from_pandas(
        sales_df,
        schema=DataTypes.ROW([
            DataTypes.FIELD("index", DataTypes.BIGINT()),
            DataTypes.FIELD("Store_ID", DataTypes.INT()),
            DataTypes.FIELD("Date", DataTypes.STRING()),
            DataTypes.FIELD("Dept", DataTypes.INT()),
            DataTypes.FIELD("Weekly_Sales", DataTypes.FLOAT())
        ])
    )
    
    extra_data = t_env.from_pandas(
        extra_df,
        schema=DataTypes.ROW([
            DataTypes.FIELD("index", DataTypes.BIGINT()),
            DataTypes.FIELD("CPI", DataTypes.FLOAT()),
            DataTypes.FIELD("Unemployment", DataTypes.FLOAT())
        ])
    )
    
    return grocery_sales, extra_data


def extract(t_env: TableEnvironment, sales_table: Table, extra_table: Table) -> Table:
    t_env.create_temporary_view("sales", sales_table)
    t_env.create_temporary_view("extra", extra_table)
        
    return t_env.sql_query("""
        SELECT 
            s.Store_ID,
            s.`Date`,
            s.Dept,
            s.Weekly_Sales,
            e.CPI,
            e.Unemployment
        FROM sales s
        LEFT JOIN extra e ON s.`index` = e.`index`
    """)

def transform(t_env: TableEnvironment, table: Table) -> Table:
    t_env.create_temporary_view("transform_source", table)
    
    return t_env.sql_query("""
        WITH filtered_data AS (
            SELECT 
                Store_ID,
                CAST(SUBSTRING(`Date`, 6, 2) AS INT) as `month`,
                Dept,
                CAST(Weekly_Sales AS DECIMAL(10,2)) as Weekly_Sales,
                CAST(CPI AS DECIMAL(10,2)) as CPI,
                CAST(Unemployment AS DECIMAL(10,2)) as Unemployment,
                `Date`
            FROM transform_source
            WHERE Store_ID IN (1, 2)
                AND Weekly_Sales IS NOT NULL
                AND `Date` IS NOT NULL
                AND Weekly_Sales > 10000
        )
        SELECT 
            Store_ID,
            `month`,
            Dept,
            Weekly_Sales,
            CPI,
            Unemployment
        FROM filtered_data
        ORDER BY `Date`
    """)

def avg_monthly_sales(t_env: TableEnvironment, table: Table) -> Table:
    t_env.create_temporary_view("agg_source", table)
    
    return t_env.sql_query("""
        SELECT 
            `month`,
            CAST(AVG(Weekly_Sales) AS DECIMAL(10,2)) as Avg_Sales
        FROM agg_source
        GROUP BY `month`
        ORDER BY `month`
    """)

def load(t_env: TableEnvironment, table1: Table, table2: Table, path1: str, path2: str):
    clean_df = table1.to_pandas().reset_index(drop=True)
    agg_df = table2.to_pandas().reset_index(drop=True)
    
    clean_df.to_csv(path1, index=True, index_label='index')
    agg_df.to_csv(path2, index=True, index_label='index')

if __name__ == "__main__":
    data_dir = '../data/'    
    clean_data_path = data_dir + 'clean_data_flink.csv'
    agg_data_path = data_dir + 'agg_data_flink.csv'
    parquet_path = data_dir + 'extra_data.parquet'    
    # Create Flink environment
    t_env = create_flink_env()
    
    grocery_sales, extra_data = import_data(t_env, '../data/walmart.db', parquet_path)
    merged_data = extract(t_env, grocery_sales, extra_data)
    clean_data = transform(t_env, merged_data)  # Pass t_env here
    agg_data = avg_monthly_sales(t_env, clean_data)  # Pass t_env here   
    load(t_env, clean_data, agg_data, clean_data_path, agg_data_path)
    
    print("\nFinal pipeline status: Success")
