import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging
import os
import pandas as pd

from walmart_pipeline.transforms.parse_transforms import ParseCSVTransform, ParseParquetTransform
from walmart_pipeline.transforms.merge_transforms import MergeTransform
from walmart_pipeline.transforms.sales_transforms import SalesTransform
from walmart_pipeline.transforms.io_transforms import WriteToGCSTransform
from walmart_pipeline.schemas import CLEAN_SCHEMA, AGG_SCHEMA, CLEAN_CSV_HEADERS, AGG_CSV_HEADERS
from walmart_pipeline.transforms.utils import validate_gcs_paths, round_floats, AddSequentialIndex 

def run_pipeline():

    """
    Executes a Dataflow pipeline that processes Walmart sales data.
    
    The pipeline performs the following operations:
    1. Reads and parses data from CSV and Parquet files
    2. Merges and cleans the data
    3. Writes clean data to BigQuery and GCS
    4. Aggregates sales data
    5. Writes aggregated data to BigQuery and GCS
    6. Validates output files in GCS
    """
    
    # Setup configuration for the pipeline
    setup_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'setup.py'))
    
    # Configure pipeline options for Google Cloud Dataflow
    options = PipelineOptions(
        project='nico-playground-384514',
        temp_location='gs://walmart-7890837/temp',
        region='us-east1',
        runner='DataflowRunner',
        setup_file=setup_file,  # Add setup file
        save_main_session=True  # Save main session
    )

    # Set additional options
    setup_options = options.view_as(SetupOptions)
    setup_options.setup_file = setup_file
    setup_options.save_main_session = True

    with beam.Pipeline(options=options) as p:
        # Stage 1: Data Ingestion
        # Process CSV data: read, parse, and prepare for merging
        csv_data = (
            p 
            | 'Read CSV' >> beam.io.ReadFromText('gs://walmart-7890837/grocery_sales.csv', skip_header_lines=1)
            | 'Parse CSV' >> ParseCSVTransform()
            | 'Log CSV' >> beam.Map(lambda x: (logging.info(f"CSV Record: {x}"), x)[1])  # Add logging
            | 'Key CSV by Index' >> beam.Map(lambda x: (x['index'], x))
        )

        # Process Parquet data: read, parse, and prepare for merging
        parquet_data = (
            p
            | 'Read Parquet' >> beam.io.ReadFromParquet('gs://walmart-7890837/extra_data.parquet')
            | 'Parse Parquet' >> ParseParquetTransform()
            | 'Log Parquet' >> beam.Map(lambda x: (logging.info(f"Parquet Record: {x}"), x)[1])  # Add logging
            | 'Key Parquet by Index' >> beam.Map(lambda x: (x['index'], x))
        )
    
        # Stage 2: Data Cleaning and Transformation
        # Merge CSV and Parquet data, clean and standardize data types
        clean_data = (
            {'csv': csv_data, 'parquet': parquet_data}
            | 'Merge Data' >> MergeTransform()
            | 'Count Records' >> beam.Map(lambda x: (logging.info(f"Processing record: {x['index']}"), x)[1])
            | 'Select Columns' >> beam.Map(lambda x: {
                'Store_ID': int(x['Store_ID']),  # Ensure integer
                'Month': int(pd.to_datetime(x['Date']).month),  # Ensure integer
                'Dept': int(x['Dept']),  # Ensure integer
                'IsHoliday': int(x.get('IsHoliday', 0)),  # Ensure boolean
                'Weekly_Sales': float(x['Weekly_Sales']),  # Ensure float
                'CPI': float(x.get('CPI', 0.0)),  # Ensure float
                'Unemployment': float(x.get('Unemployment', 0.0))  # Ensure float
            })
        )

        # Stage 3: Data Output
        # Write clean data to BigQuery and GCS  
        _ = (
            clean_data
            | 'Round Clean Data' >> beam.Map(round_floats)  # Add rounding
            | 'Write Clean to BQ' >> beam.io.WriteToBigQuery(
                'nico-playground-384514:clean_walmart.clean',
                schema=CLEAN_SCHEMA,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE',
                method='FILE_LOADS',
                temp_file_format='AVRO'
            )
        )
        # Write cleaned data to GCS
        _ = (
            clean_data
            | 'Write Clean to GCS' >> WriteToGCSTransform(
                'gs://walmart-7890837/clean_output.csv',
                CLEAN_CSV_HEADERS
            )
        )

      # Stage 4: Data Aggregation and Final Output
        # Process sales data for aggregated metrics
        sales_data = clean_data | 'Process Sales' >> SalesTransform()

         # Write aggregated data to BigQuery
        _ = (
            sales_data
            | 'Round Agg Data' >> beam.Map(round_floats)  # Add rounding
            | 'Write Agg to BQ' >> beam.io.WriteToBigQuery(
                'nico-playground-384514:agg_walmart.aggregated',
                schema=AGG_SCHEMA,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE',
                    # Add these parameters:
                method='FILE_LOADS',
                temp_file_format='AVRO'
            )
        )
        # Write aggregated data to GCS
        _ = (
            sales_data
            | 'Write Agg to GCS' >> WriteToGCSTransform(
                'gs://walmart-7890837/agg_output.csv',
                AGG_CSV_HEADERS
            )
        )    
    # Stage 5: Output Validation
    bucket_name = 'walmart-7890837'
    validation_result = validate_gcs_paths(
        bucket_name,
        ['clean_output.csv', 'aggregated_output.csv']
    )
    logging.info(f"Output validation result: {validation_result}")

if __name__ == '__main__':
    # Configure logging and execute pipeline
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()