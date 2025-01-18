from airflow import DAG
from google.api_core.exceptions import NotFound
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import numpy as np
import tempfile
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_data(df, stage_name):
    """Validate data and log issues"""
    logger.info(f"\n=== Data Validation for {stage_name} ===")
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Columns: {df.columns.tolist()}")
    
    # Check for null values
    null_counts = df.isnull().sum()
    if null_counts.any():
        logger.warning(f"Null value counts:\n{null_counts[null_counts > 0]}")
    
    # Check for None values (explicit None, not NaN)
    none_counts = df.applymap(lambda x: x is None).sum()
    if none_counts.any():
        logger.warning(f"None value counts:\n{none_counts[none_counts > 0]}")
    
    # Check for zero values in numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        zero_count = (df[col] == 0).sum()
        if zero_count > 0:
            logger.warning(f"Found {zero_count} zero values in column {col}")

def read_from_gcs(bucket_name, file_name):
    """Read file from GCS"""
    logger.info(f"Reading {file_name} from {bucket_name}")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    with tempfile.NamedTemporaryFile() as temp_file:
        blob.download_to_filename(temp_file.name)
        if file_name.endswith('.parquet'):
            df = pd.read_parquet(temp_file.name)
        else:
            df = pd.read_csv(temp_file.name)
    return df

def process_sales(**context):
    """Process sales data with forward fill"""
    logger.info("=== Processing Sales Data ===")
    try:
        # Read data
        df = read_from_gcs('walmart-7890837', 'grocery_sales.csv')
        
        # Log zero values before processing
        zero_sales = df[df['Weekly_Sales'] == 0]
        if not zero_sales.empty:
            logger.warning(f"Found {len(zero_sales)} zero values in Weekly_Sales before processing:")
            logger.warning(f"Zero sales records:\n{zero_sales}")
        
        # Convert data types
        df['index'] = pd.to_numeric(df['index'], errors='coerce')
        df['Store_ID'] = pd.to_numeric(df['Store_ID'], errors='coerce')
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Date'] = df['Date'].ffill()  # Forward fill null dates
        df['Weekly_Sales'] = pd.to_numeric(df['Weekly_Sales'], errors='coerce')
        
        # Validate Store_ID
        df = df[df['Store_ID'].isin([1, 2])]
        
        # Consider replacing zeros with NaN and then forward fill
        df.loc[df['Weekly_Sales'] == 0, 'Weekly_Sales'] = np.nan
        df['Weekly_Sales'] = df['Weekly_Sales'].ffill()
        
        # Convert Timestamp to string format before returning
        df['Date'] = pd.to_datetime(df['Date'])
        
        logger.info(f"Processed sales data shape: {df.shape}")
        
        # Final validation
        zero_sales_after = df[df['Weekly_Sales'] == 0]
        if not zero_sales_after.empty:
            logger.warning(f"Still found {len(zero_sales_after)} zero values in Weekly_Sales after processing:")
            logger.warning(f"Remaining zero sales records:\n{zero_sales_after}")
        
        # Convert DataFrame to dict before returning
        return df
        
    except Exception as e:
        logger.error(f"Sales processing failed: {str(e)}")
        raise

def process_economic(**context):
    """Process economic data with forward fill"""
    logger.info("=== Processing Economic Data ===")
    try:
        # Read data
        df = read_from_gcs('walmart-7890837', 'extra_data.parquet')
        
        # Convert data types
        df['index'] = pd.to_numeric(df['index'], errors='coerce')
        df['CPI'] = pd.to_numeric(df['CPI'], errors='coerce')
        df['Unemployment'] = pd.to_numeric(df['Unemployment'], errors='coerce')
        df['IsHoliday'] = df['IsHoliday'].astype(bool)
        
        # Forward fill missing values
        df[['CPI', 'Unemployment', 'MarkDown4', 'MarkDown5', 'Type', 'Size']] = df[['CPI', 'Unemployment', 'MarkDown4', 'MarkDown5', 'Type', 'Size']].ffill()
        
        logger.info(f"Processed economic data shape: {df.shape}")
        
        # Convert DataFrame to dict before returning
        return df
        
    except Exception as e:
        logger.error(f"Economic processing failed: {str(e)}")
        raise

def merge_datasets(**context):
    """Merge processed datasets"""
    logger.info("=== Merging Datasets ===")
    try:
        sales_df = process_sales(**context)
        economic_df = process_economic(**context)
       
        logger.info(f"Sales DataFrame shape: {sales_df.shape}")
        logger.info(f"Economic DataFrame shape: {economic_df.shape}")
        
        # Perform merge
        merged_df = pd.merge(sales_df, economic_df, on='index', how='inner')
        logger.info(f"Merged data shape: {merged_df.shape}")
        
        # Validate merged data
        validate_data(merged_df, "Merged Dataset")
        
        return merged_df
        
    except Exception as e:
        logger.error(f"Merge failed: {str(e)}")
        raise

def output_clean_data(**context):
    """Output clean merged data to GCS and BigQuery"""
    logger.info("=== Outputting Data ===")
    try:
        # Transform data for clean output
        merged_df = merge_datasets(**context)
        merged_df['Month'] = merged_df['Date'].dt.month
        
        # Select required columns
        clean_df = merged_df[['Store_ID', 'Month', 'Dept', 'IsHoliday', 
                           'Weekly_Sales', 'CPI', 'Unemployment']]
        clean_df = clean_df.reset_index(drop=False)  # Keep the index as a column

        
        logger.info(f"Clean DataFrame shape: {clean_df.shape}")
        logger.info(f"Clean DataFrame columns: {clean_df.columns.tolist()}")
        
        # Output to GCS
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            clean_df.to_csv(temp_file.name, index=False)
            logger.info(f"Outputting clean data to GCS: {temp_file.name}")
            
            storage_client = storage.Client()
            bucket = storage_client.bucket('walmart-7890837')
            blob = bucket.blob('clean_airflow_output.csv')
            blob.upload_from_filename(temp_file.name)
        
        # Write to BigQuery
        logger.info("Writing to BigQuery...")
        client = bigquery.Client()
        
        # Define dataset and table references
        dataset_id = "clean_airflow_walmart"
        project_id = "nico-playground-384514"
        table_id = "clean"
        
        dataset_ref = f"{project_id}.{dataset_id}"
        table_ref = f"{dataset_ref}.{table_id}"
        
        # Create dataset if it doesn't exist
        try:
            client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
        except NotFound:
            logger.info(f"Dataset {dataset_ref} not found, creating...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Specify the location
            client.create_dataset(dataset)
            logger.info(f"Dataset {dataset_ref} created successfully")
        
        # Create table schema
        schema = [
            bigquery.SchemaField("Store_ID", "INTEGER"),
            bigquery.SchemaField("Month", "INTEGER"),
            bigquery.SchemaField("Dept", "INTEGER"),
            bigquery.SchemaField("IsHoliday", "BOOLEAN"),
            bigquery.SchemaField("Weekly_Sales", "FLOAT"),
            bigquery.SchemaField("CPI", "FLOAT"),
            bigquery.SchemaField("Unemployment", "FLOAT")
        ]
        
        # Create table if it doesn't exist
        try:
            client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists")
        except NotFound:
            logger.info(f"Table {table_ref} not found, creating...")
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            logger.info(f"Table {table_ref} created successfully")
        
        # Load data into BigQuery
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(clean_df, table_ref, job_config=job_config)
        job.result()
        logger.info("BigQuery write successful")
        
        logger.info("Data output successful.")
        return clean_df  # Add this return statement

        
    except Exception as e:
        logger.error(f"Output failed: {str(e)}")
        raise

def output_aggregated_data(**context):
    """Output aggregated sales data to GCS and BigQuery"""
    logger.info("=== Outputting Aggregated Data ===")
    try:
        output_clean = output_clean_data(**context)
        agg_df = output_clean.groupby('Month')['Weekly_Sales'].mean().reset_index()
        agg_df = agg_df.rename(columns={'Weekly_Sales': 'Average_Weekly_Sales'})
        agg_df['Average_Weekly_Sales'] = agg_df['Average_Weekly_Sales'].round(2)
        
        # Add index for CSV output
        agg_df_with_index = agg_df.reset_index(drop=False)
        
        logger.info(f"Aggregated DataFrame shape: {agg_df.shape}")
        logger.info(f"Aggregated DataFrame columns: {agg_df.columns.tolist()}")
        
        # Output to GCS (with index)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            agg_df_with_index.to_csv(temp_file.name, index=False)
            logger.info(f"Outputting aggregated data to GCS: {temp_file.name}")
            
            storage_client = storage.Client()
            bucket = storage_client.bucket('walmart-7890837')
            blob = bucket.blob('agg_airflow_output.csv')
            blob.upload_from_filename(temp_file.name)
        
        # Write to BigQuery (without index)
        logger.info("Writing to BigQuery...")
        client = bigquery.Client()
        
        # Define dataset and table references
        dataset_id = "agg_airflow_walmart"
        project_id = "nico-playground-384514"
        table_id = "aggregated"
        
        dataset_ref = f"{project_id}.{dataset_id}"
        table_ref = f"{dataset_ref}.{table_id}"
        
        # Create dataset if it doesn't exist
        try:
            client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
        except NotFound:
            logger.info(f"Dataset {dataset_ref} not found, creating...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            client.create_dataset(dataset)
            logger.info(f"Dataset {dataset_ref} created successfully")
        
        # Create table schema (without index)
        schema = [
            bigquery.SchemaField("Month", "INTEGER"),
            bigquery.SchemaField("Average_Weekly_Sales", "FLOAT")
        ]
        
        # Create table if it doesn't exist
        try:
            client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists")
        except NotFound:
            logger.info(f"Table {table_ref} not found, creating...")
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            logger.info(f"Table {table_ref} created successfully")
        
        # Load data into BigQuery (without index)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(agg_df, table_ref, job_config=job_config)
        job.result()
        logger.info("BigQuery write successful")
        
        logger.info("Aggregated data output successful.")
        return agg_df
        
    except Exception as e:
        logger.error(f"Aggregated output failed: {str(e)}")
        raise
    
def validate_outputs(**context):
    """Validate that both output files exist in GCS"""
    logger.info("=== Validating Outputs ===")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket('walmart-7890837')
        
        # Check for both files
        clean_blob = bucket.blob('clean_airflow_output.csv')
        agg_blob = bucket.blob('agg_airflow_output.csv')
        
        clean_exists = clean_blob.exists()
        agg_exists = agg_blob.exists()
        
        if clean_exists and agg_exists:
            logger.info("Both output files found in GCS")
            return True
        else:
            missing_files = []
            if not clean_exists:
                missing_files.append('clean_airflow_output.csv')
            if not agg_exists:
                missing_files.append('agg_airflow_output.csv')
            logger.error(f"Missing files in GCS: {missing_files}")
            return False
            
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        raise

    # DAG definition
with DAG(
    'walmart_sales_etl',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'execution_timeout': timedelta(minutes=10),  # Global timeout
        'pool': 'default_pool'
    },
    description='Walmart sales ETL pipeline',
    schedule_interval='0 5 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent multiple runs
    concurrency=2,      # Limit concurrent tasks
    tags=['walmart', 'sales'],
) as dag:
    
    process_sales_task = PythonOperator(
        task_id='process_sales',
        python_callable=process_sales,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),  # Kill if takes too long
        retries=1,
        retry_delay=timedelta(minutes=1),
        pool='default_pool',
        pool_slots=1,  # Limit resource usage
        dag=dag
    )
    
    process_economic_task = PythonOperator(
        task_id='process_economic',
        python_callable=process_economic,
        do_xcom_push=False,
    )
    
    merge_datasets_task = PythonOperator(
        task_id='merge_datasets',
        python_callable=merge_datasets,
        do_xcom_push=False,
    )
    
    output_clean = PythonOperator(
        task_id='output_clean_data',
        python_callable=output_clean_data,
        do_xcom_push=False,
        retries=2,
    )
    
    output_agg = PythonOperator(
        task_id='output_aggregated_data',
        python_callable=output_aggregated_data,
        do_xcom_push=False,
        retries=2,
    )
    
    validate = PythonOperator(
        task_id='validate_outputs',
        python_callable=validate_outputs,
        do_xcom_push=False,
        retries=2,
    )
    
    
    [process_sales_task, process_economic_task] >> merge_datasets_task >> output_clean >> output_agg >> validate
