import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, month
from pyspark.sql.types import FloatType 
from pyspark.sql import SQLContext, Row
import os
import pandas as pd
import sqlite3
from datetime import datetime
import numpy as np
import shutil

class WalmartSalesProcessor:
    def __init__(self, spark_context, spark_session):
        self.sc = spark_context
        self.spark = spark_session
        
    def read_sqlite_to_rdd(self, database_path, table_name):
        conn = sqlite3.connect(database_path)
        df = pd.read_sql_query(f'SELECT * FROM {table_name}', conn)
        conn.close()
        return self.sc.parallelize(df.to_dict('records'))

    @staticmethod
    def replace_null_values(rdd):
        last_valid_date = None
        last_valid_sales = None
        last_valid_unemployment = None
        cleaned_rows = []
        
        for row in rdd:
            # Handle Date
            date_value = row.get('Date')
            if date_value in ['null', None]:
                row['Date'] = last_valid_date
            else:
                try:
                    last_valid_date = datetime.fromisoformat(str(date_value))
                    row['Date'] = last_valid_date
                except (ValueError, TypeError):
                    row['Date'] = last_valid_date
            
            # Handle Weekly_Sales
            sales_value = row.get('Weekly_Sales')
            if sales_value in ['null', None]:
                row['Weekly_Sales'] = last_valid_sales
            else:
                try:
                    last_valid_sales = float(sales_value)
                    row['Weekly_Sales'] = last_valid_sales
                except (ValueError, TypeError):
                    row['Weekly_Sales'] = last_valid_sales
                
            # Handle Unemployment
            unemployment_value = row.get('Unemployment')
            if unemployment_value in ['null', None]:
                row['Unemployment'] = last_valid_unemployment
            else:
                try:
                    last_valid_unemployment = float(unemployment_value)
                    row['Unemployment'] = last_valid_unemployment
                except (ValueError, TypeError):
                    row['Unemployment'] = last_valid_unemployment
            
            cleaned_rows.append(row)
        
        return cleaned_rows

    @staticmethod
    def transform_to_key_value(row):
        index = row['index']
        value = {k: v for k, v in row.items() if k != 'index'}
        return (index, value)

    @staticmethod
    def flatten_result(record):
        key, (dict1, dict2) = record
        values = {
            'Store_ID': dict1['Store_ID'],
            'Date': dict1['Date'],
            'Dept': dict1['Dept'],
            'Weekly_Sales': dict1['Weekly_Sales'],
            'IsHoliday': dict2['IsHoliday'],
            'Temperature': dict2['Temperature'],
            'Fuel_Price': dict2['Fuel_Price'],
            'MarkDown1': dict2['MarkDown1'],
            'MarkDown2': dict2['MarkDown2'],
            'MarkDown3': dict2['MarkDown3'],
            'MarkDown4': dict2['MarkDown4'],
            'MarkDown5': dict2['MarkDown5'],
            'CPI': dict2['CPI'],
            'Unemployment': dict2['Unemployment'],
            'Type': dict2['Type'],
            'Size': dict2['Size']
        }
        return (key, values)

    @staticmethod
    def extract_month_from_flattened(record):
        key, values = record
        date = values['Date']
        month = date.month if date is not None else None
        values['Month'] = month
        return (key, values)

    @staticmethod
    def filter_high_sales(record):
        key, values = record
        return values['Weekly_Sales'] > 10000

    @staticmethod
    def select_columns(record):
        key, values = record
        selected_values = {
            'Store_ID': values['Store_ID'],
            'Month': values['Month'],
            'Dept': values['Dept'],
            'IsHoliday': values['IsHoliday'],
            'Weekly_Sales': values['Weekly_Sales'],
            'CPI': values['CPI'],
            'Unemployment': values['Unemployment']
        }
        return (key, selected_values)

    def process_data(self, database_path, parquet_path):
        # Read and clean grocery sales data
        grocery_sales_rdd = self.read_sqlite_to_rdd(database_path, 'grocery_sales')
        cleaned_rdd = grocery_sales_rdd.mapPartitions(self.replace_null_values)
        
        # Read and clean extra data
        extra_data_rdd = self.spark.read.parquet(parquet_path).rdd.map(lambda r: r.asDict())
        extra_data_cleaned = extra_data_rdd.mapPartitions(self.replace_null_values)
        
        # Transform and join data
        key_value_grocery_rdd = cleaned_rdd.map(self.transform_to_key_value)
        key_value_extra_rdd = extra_data_cleaned.map(self.transform_to_key_value)
        
        # Process and transform joined data
        joined_rdd = key_value_grocery_rdd.join(key_value_extra_rdd)
        flattened_rdd = joined_rdd.map(self.flatten_result)
        clean_rdd = flattened_rdd.map(self.extract_month_from_flattened)
        filtered_sales_rdd = clean_rdd.filter(self.filter_high_sales)
        filtered_rdd = filtered_sales_rdd.map(self.select_columns)
        
        return filtered_rdd

    def calculate_monthly_averages(self, filtered_rdd):
        month_sales_rdd = filtered_rdd.map(lambda x: (x[1]['Month'], x[1]['Weekly_Sales']))
        grouped_sales_rdd = month_sales_rdd.groupByKey()
        return grouped_sales_rdd.mapValues(lambda sales: round(sum(sales) / len(sales), 2))

    @staticmethod
    def save_rdds_as_csv(filtered_rdd, average_sales_rdd, filtered_rdd_path, average_sales_rdd_path):
        # Remove existing directories if they exist
        for path in [filtered_rdd_path, average_sales_rdd_path]:
            if os.path.exists(path):
                shutil.rmtree(path)
        
        # Save the RDDs as CSV files
        filtered_rdd.saveAsTextFile(filtered_rdd_path)
        average_sales_rdd.saveAsTextFile(average_sales_rdd_path)

    @staticmethod
    def validate_file_paths(filtered_rdd_path, average_sales_rdd_path):
        filtered_exists = os.path.exists(filtered_rdd_path)
        average_sales_exists = os.path.exists(average_sales_rdd_path)
        return filtered_exists and average_sales_exists

def main():
    # Initialize Spark
    try:
        sc = ps.SparkContext('local[*]')
    except:
        print('spark context already exists')
    spark = ps.sql.SparkSession(sc)
    
    # Initialize processor
    processor = WalmartSalesProcessor(sc, spark)
    
    # Define paths
    data_dir = '../data/'
    database_path = f'{data_dir}walmart.db'
    parquet_path = f'{data_dir}extra_data.parquet'
    clean_data_path = f'{data_dir}clean_data_rdd.csv'
    agg_data_path = f'{data_dir}agg_data_rdd.csv'
    
    # Process data
    filtered_rdd = processor.process_data(database_path, parquet_path)
    average_sales_rdd = processor.calculate_monthly_averages(filtered_rdd)
    
    # Prepare for CSV export
    filtered_rdd_csv = filtered_rdd.map(lambda x: ','.join(map(str, [
        x[1]['Store_ID'], x[1]['Month'], x[1]['Dept'], x[1]['IsHoliday'],
        x[1]['Weekly_Sales'], x[1]['CPI'], x[1]['Unemployment']
    ])))
    average_sales_rdd_csv = average_sales_rdd.map(lambda x: f"{x[0]},{x[1]:.2f}")
    
    # Save and validate
    processor.save_rdds_as_csv(filtered_rdd_csv, average_sales_rdd_csv, clean_data_path, agg_data_path)
    files_exist = processor.validate_file_paths(clean_data_path, agg_data_path)
    print(f"Do both output files exist? {files_exist}")

if __name__ == "__main__":
    main()