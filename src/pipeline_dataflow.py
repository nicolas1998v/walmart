import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import WriteToText
import logging
from apache_beam.transforms.util import WithKeys


# Set up logging to show info-level messages
logging.basicConfig(level=logging.INFO)


# Define a DoFn class that handles both parsing and filling "null" values for CSV
class ParseDataDoFn(beam.DoFn):
    def __init__(self):
        # Initialize dictionaries to store last valid values for forward filling
        self.last_valid_values = {}  # To store the last valid value for each key (including 'Date')

    def process(self, row):
       
        row = self._parse_csv_line(row)
           # Perform forward-filling for both CSV and Parquet data
        row = self._fill_missing_values(row)
            # Yield the processed row
        yield row

    def _parse_csv_line(self, line):
        """Helper function to parse a CSV line."""
        import csv
        from io import StringIO
        reader = csv.DictReader(StringIO(line), fieldnames=['index', 'Store_ID', 'Date', 'Dept', 'Weekly_Sales'])
        row = next(reader)

        return row

    def _fill_missing_values(self, row):
       
        import pandas as pd

        # Fill missing 'Weekly_Sales' (forward-fill using last valid value)
        row['Weekly_Sales'] = self._fill_with_last_valid('Weekly_Sales', row.get('Weekly_Sales'), converter=float)
        # Handle missing or 'null' dates by using the last valid date
        row['Date'] = self._fill_with_last_valid('Date', row.get('Date'), converter=pd.to_datetime)
        
        return row       

    def _fill_with_last_valid(self, key, value, converter=None):
        if value == 'null' or value == 'None' or value is None:
            # If missing, use the last valid value
            last_value = self.last_valid_values.get(key)
            logging.info(f"Filling {key} with last valid value: {last_value} (was {value})")
            return last_value
        else:
            try:
                # Convert the value if it's valid and update last valid values
                value = converter(value) 
                self.last_valid_values[key] = value
                return value
            except Exception as e:
                logging.error(f"Error converting value '{value}' for key '{key}': {e}")
                return 0.0

# Parquet Parsing DoFn
class ParseParquetDoFn(beam.DoFn):
    def __init__(self):
        self.last_valid_values = {}

    def process(self, row):
        row = dict(row)
        row['index'] = str(row['index'])
        row = self._fill_missing_values(row)
        yield row

    def _fill_missing_values(self, row):
        
        for field in ['Unemployment', 'CPI']:  # Add CPI here
            row[field] = self._fill_with_last_valid(field, row.get(field), converter=float)
        return row

    def _fill_with_last_valid(self, key, value, converter=None):
        if value == 'null' or value == 'None' or value is None:
            value = self.last_valid_values.get(key)
        else:
            try:
                value = converter(value)
                self.last_valid_values[key] = value
            except Exception as e:
                print(f"Error converting value '{value}' for key '{key}': {e}")
                value = None
        return value    

def merge_records(records):
    # Unpack the tuple returned by CoGroupByKey
    key, grouped_records = records  # key is the common key (index), grouped_records is a dict

    # Extract CSV and Parquet records from the grouped_records dictionary
    csv_records = grouped_records.get('csv', [])
    parquet_records = grouped_records.get('parquet', [])

    # If there is no CSV record (i.e., no matching grocery sales data), skip merging
    if not csv_records:
        return None  # Skip this record entirely

    # Get the first (and possibly only) record from both CSV and Parquet
    csv_record = csv_records[0] if csv_records else {}
    parquet_record = parquet_records[0] if parquet_records else {}

    # Merge CSV and Parquet records
    merged = {**csv_record, **parquet_record}
    
    return merged if merged else None

def extract_month(record):
    try:
        #print(f"Processing record: {record}")  # Print each record for debugging
        # Extract the month from the 'Date' column
        record['Month'] = record['Date'].month
        #print(f"Extracted Month: {record['Month']} from Date: {record['Date']}")
        return record
    except Exception as e:
        # Log the error and the record that caused it
        print(f"Error processing record: {record}. Error: {e}")

def filter_sales(record):
    # Ensure that record is not None and that 'Weekly_Sales' exists and is a valid number
    return record and 'Weekly_Sales' in record and record['Weekly_Sales'] is not None and record['Weekly_Sales'] > 10000

def select_columns(record):
    return {
        'Store_ID': record['Store_ID'],
        'Month': record['Month'],
        'Dept': record['Dept'],
        'IsHoliday': record['IsHoliday'],
        'Weekly_Sales': record['Weekly_Sales'],
        'CPI': record['CPI'],
        'Unemployment': record['Unemployment']
    }

def calculate_average_sales(group):
    month, records = group
    total_sales = sum(r['Weekly_Sales'] for r in records)
    avg_sales = round(total_sales / len(records), 2)
    return {'Month': month, 'Average_Weekly_Sales': avg_sales}

class AddSequentialIndexDoFn(beam.DoFn):
    def __init__(self):
        self.index = -1

    def start_bundle(self):
        self.index = -1  # Reset the index for each bundle

    def process(self, element):
        self.index += 1
        yield (self.index, element)

def format_clean_csv(index, record):
    """Format clean data for CSV output, with a clean index."""
    return f"{index},{record['Store_ID']},{record['Month']},{record['Dept']},{record.get('IsHoliday', '')},{record['Weekly_Sales']},{record.get('CPI', '')},{record.get('Unemployment', '')}"


def format_agg_csv(index, record):
    """Format aggregated data for CSV output, with month-based index."""
    return f"{index},{record['Month']},{record['Average_Weekly_Sales']}"

def validate_gcs_paths(bucket_name, paths):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    
    # Strip the gs://bucket-name/ prefix from paths
    for path in paths:
        blob_name = path.replace(f'gs://{bucket_name}/', '')
        blob = bucket.blob(blob_name)
        exists = blob.exists()
        logging.info(f"Checking {blob_name}: {'exists' if exists else 'does not exist'}")
        if not exists:
            return False
    return True

# Define the Dataflow Pipeline
def run_pipeline():
    pipeline_options = PipelineOptions(
        project='nico-playground-384514',
        temp_location='gs://walmart-7890837/temp',
        region='us-east1',  # Dallas region - newer and often less utilized     
        runner='DataFlowRunner'
    )

    with beam.Pipeline(options=pipeline_options) as p:
         
        # Read and parse CSV
        csv_pcoll = (
            p
            | 'Read CSV' >> beam.io.ReadFromText('gs://walmart-7890837/grocery_sales.csv', skip_header_lines=1)
            | 'Parse CSV and Fill Nulls' >> beam.ParDo(ParseDataDoFn()) 
        )        
        
        # Read and parse Parquet
        parquet_pcoll = (
            p
            | 'Read Parquet' >> beam.io.ReadFromParquet('gs://walmart-7890837/extra_data.parquet')
            | 'Parse Parquet' >>  beam.ParDo(ParseParquetDoFn())
            | 'Key By Index (Parquet)' >> beam.Map(lambda x: (x['index'], x))
        )
         # Key CSV by 'index' for merging
        keyed_csv_pcoll = csv_pcoll | 'Key By Index (CSV)' >> beam.Map(lambda x: (x['index'], x))


        # Merge CSV and Parquet on 'index'
        merged_pcoll = (
            {'csv': keyed_csv_pcoll, 'parquet': parquet_pcoll}
            | 'Merge on index' >> beam.CoGroupByKey()
            | 'Flatten Merged Data' >> beam.Map(merge_records)
            | 'Filter Out Empty Merged Records' >> beam.Filter(lambda x: x is not None)
            | 'Log Clean Data Inline ' >> beam.Map(lambda x: (logging.info(f"Merged Row: {x}"), x)[1])

        )
       
        # Extract Month and Filter by Sales
        filtered_pcoll = (
            merged_pcoll
            | 'Extract Month' >> beam.Map(extract_month)
            | 'Filter Sales' >> beam.Filter(filter_sales)
        )
        
        # Select relevant columns
        clean_pcoll = (filtered_pcoll | 'Select Columns' >> beam.Map(select_columns)
                | 'Debug Clean PCollection' >> beam.Map(lambda x: (
                logging.info(f"Clean PColl Record: {x}"),
                x)[1])
        )   
        
        # Calculate aggregated sales
        aggregated_pcoll = (
            clean_pcoll
            | 'Group By Month' >> beam.GroupBy(lambda x: x['Month'])
            | 'Calculate Average Sales' >> beam.Map(calculate_average_sales)
            #| 'Log Clean Data Inline 2' >> beam.Map(lambda x: (logging.info(f"Clean Row: {x}"), x)[1])
        )

        # Final transformations for clean data
        clean_with_index = (
            clean_pcoll
            | 'Add Sequential Index to Clean Data' >> beam.ParDo(AddSequentialIndexDoFn())
            | 'Format Clean to CSV' >> beam.MapTuple(format_clean_csv))

        
        # Define headers
        CSV_HEADER_CLEAN = "index,Store_ID,Month,Dept,IsHoliday,Weekly_Sales,CPI,Unemployment"
        CSV_HEADER_AGG = "index,Month,Average_Weekly_Sales"


        # Write clean data to GCS with header at the beginning
        clean_output = (
            (p | 'Create Clean Header' >> beam.Create([CSV_HEADER_CLEAN]), clean_with_index)
            | "Combine Clean Header and Rows" >> beam.Flatten()
            | "Write Clean Data to GCS" >> beam.io.WriteToText("gs://walmart-7890837/clean_output.csv", shard_name_template="")
        )

        # Final transformations for aggregated data
        aggregated_with_index = ( aggregated_pcoll
            | 'Sort by Month' >> beam.transforms.combiners.Top.Smallest(12, key=lambda x: x['Month'])
            | 'Flatten Sorted Aggregated Data' >> beam.FlatMap(lambda elements: elements)
            | 'Add Sequential Index to Aggregated Data' >> beam.ParDo(AddSequentialIndexDoFn())
            | 'Format Aggregated to CSV' >> beam.MapTuple(format_agg_csv)
        )

        # Write aggregated data to GCS with header at the beginning
        agg_output = (
            (p | 'Create Aggregated Header' >> beam.Create([CSV_HEADER_AGG]), aggregated_with_index)
            | "Combine Aggregated Header and Rows" >> beam.Flatten()
            | "Write Aggregated Data to GCS" >> beam.io.WriteToText("gs://walmart-7890837/aggregated_output.csv", shard_name_template="")
        )

        # Debug and write clean data to BigQuery
        (clean_pcoll 
            | 'Debug Clean Before BQ' >> beam.Map(lambda x: (print(f"Clean record before BQ: {x}"), x)[1])
            | 'Format Clean for BigQuery' >> beam.Map(lambda x: {
                'Store_ID': int(x['Store_ID']),
                'Month': int(x['Month']),
                'Dept': int(x['Dept']),
                'IsHoliday': int(x['IsHoliday']),
                'Weekly_Sales': float(x['Weekly_Sales']),
                'CPI': float(x['CPI']),
                'Unemployment': float(x['Unemployment'])
            })
            | 'Debug Clean After Format' >> beam.Map(lambda x: (print(f"Clean record after format: {x}"), x)[1])
            | 'Write Clean to BigQuery' >> beam.io.WriteToBigQuery(
                table=f'nico-playground-384514:clean_walmart.clean',
                schema={
                    'fields': [
                        {'name': 'Store_ID', 'type': 'INTEGER'},
                        {'name': 'Month', 'type': 'INTEGER'},
                        {'name': 'Dept', 'type': 'INTEGER'},
                        {'name': 'IsHoliday', 'type': 'INTEGER'},
                        {'name': 'Weekly_Sales', 'type': 'FLOAT'},
                        {'name': 'CPI', 'type': 'FLOAT'},
                        {'name': 'Unemployment', 'type': 'FLOAT'}
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method='FILE_LOADS'
            ))

        # Debug and write aggregated data to BigQuery
        (aggregated_pcoll
            | 'Debug Agg Before BQ' >> beam.Map(lambda x: (print(f"Aggregated record before BQ: {x}"), x)[1])
            | 'Format Aggregated for BigQuery' >> beam.Map(lambda x: {
                'Month': int(x['Month']),
                'Average_Weekly_Sales': float(x['Average_Weekly_Sales'])
            })
            | 'Debug Agg After Format' >> beam.Map(lambda x: (print(f"Aggregated record after format: {x}"), x)[1])
            | 'Write Aggregated to BigQuery' >> beam.io.WriteToBigQuery(
                table=f'nico-playground-384514:agg_walmart.aggregated',
                schema={
                    'fields': [
                        {'name': 'Month', 'type': 'INTEGER'},
                        {'name': 'Average_Weekly_Sales', 'type': 'FLOAT'}
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method='FILE_LOADS'
            ))
                        # Wait for the pipeline to finish
    result = p.run()
    result.wait_until_finish()

    # Run the validation function after pipeline completes
    bucket_name = 'walmart-7890837'
    paths = ['clean_output.csv', 'aggregated_output.csv']
    validation_result = validate_gcs_paths(bucket_name, paths)
    print("Validation result:", validation_result)

if __name__ == '__main__':
    run_pipeline()