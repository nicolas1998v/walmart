import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import csv
from io import StringIO
import os

# Define a DoFn class that handles both parsing and filling "null" values for CSV
class ParseDataDoFn(beam.DoFn):
    def __init__(self):
        # Initialize dictionaries to store last valid values for forward filling
        self.last_valid_values = {}  # To store the last valid value for each key (including 'Date')

    def process(self, row):
       
        # Check if input is a CSV line or a Parquet row by inspecting its type
        if isinstance(row, str):
            # If it's a string, it's CSV data
            row = self._parse_csv_line(row)

           # Perform forward-filling for both CSV and Parquet data
        row = self._fill_missing_values(row)

            # Yield the processed row
        yield row

    def _parse_csv_line(self, line):
        """Helper function to parse a CSV line."""
        reader = csv.DictReader(StringIO(line), fieldnames=['index', 'Store_ID', 'Date', 'Dept', 'Weekly_Sales'])
        row = next(reader)

        return row

    def _fill_missing_values(self, row):
        """Helper function to fill 'null' or None values using the last valid value."""
        if "Weekly_Sales" and "Date" in row:
            # Fill missing 'Weekly_Sales' (forward-fill using last valid value)
            row['Weekly_Sales'] = self._fill_with_last_valid('Weekly_Sales', row.get('Weekly_Sales'), converter=float)

            # Handle missing or 'null' dates by using the last valid date
            row['Date'] = self._fill_with_last_valid('Date', row.get('Date'), converter=pd.to_datetime)
        if "Unemployment" in row:
             # Optionally, handle other fields as needed (CPI, Unemployment, etc.)
            row['Unemployment'] = self._fill_with_last_valid('Unemployment', row.get('Unemployment'), converter=float)
        
        return row

    def _fill_with_last_valid(self, key, value, converter=None):
        """
        Helper function to fill 'null' or None values using the last valid value.
        Applies a type converter (e.g., to float or datetime) if provided.
        """
    # Log the value before attempting any conversion
        print(f"Processing {key}: raw value = {value}")

    # If the value is 'null' or None, use the last valid value for that key
        if value == 'null' or value == 'None' or value is None:
            value = self.last_valid_values.get(key)
        else:
        # If a valid value is found, try to convert it
            try:
                value = converter(value)
                # Store the current valid value as the last valid one
                self.last_valid_values[key] = value  
            except Exception as e:
                print(f"Error converting value '{value}' for key '{key}': {e}")
                value = None  # If conversion fails, keep it as None

        return value

parse_dofn = ParseDataDoFn()

# Update the parse_parquet function to forward-fill values using the DoFn class
def parse_parquet(row):
    # Convert row into a dictionary and ensure 'index' is a string
    row = dict(row)
    row['index'] = str(row['index'])
   
    # Process the row using the DoFn
    filled_rows = list(parse_dofn.process(row))  # Collect all yielded rows

    # Return the first filled row if available
    if filled_rows:
        return filled_rows[0]  # Return the processed row
    else:
        return None  # Handle case where no rows were processed
    
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
    
    return merged

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
    return record['Weekly_Sales'] > 10000

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

def format_csv_row_clean(record):
    """Convert records into a clean CSV format with a new index and column names."""
    # Initialize a static variable to track if the header has been added
    if not hasattr(format_csv_row_clean, "header_added"):
        format_csv_row_clean.header_added = False

    # Initialize a counter for the new index (could also be done globally, or managed externally)
    if not hasattr(format_csv_row_clean, "index"):
        format_csv_row_clean.index = 0

    # Prepare the CSV output
    csv_output = []

    # Add the column names only if they haven't been added yet
    if not format_csv_row_clean.header_added:
        csv_output.append("index,Store_ID,Month,Dept,IsHoliday,Weekly_Sales,CPI,Unemployment")
        format_csv_row_clean.header_added = True

    # Print the record for debugging
    #print(f"Processing record: {record}")

    # Create a row in CSV format
    row = f"{format_csv_row_clean.index},{record['Store_ID']},{record['Month']},{record['Dept']},{record.get('IsHoliday', '')},{record['Weekly_Sales']},{record.get('CPI', '')},{record.get('Unemployment', '')}"
    csv_output.append(row)

    # Increment the index for the next record
    format_csv_row_clean.index += 1

    # Return the CSV row as a string, joined by newline characters
    return "\n".join(csv_output)

def format_csv_row_agg(record):
    """Convert the record into a clean CSV format."""
    # Initialize a static variable to track if the header has been added
    if not hasattr(format_csv_row_agg, "header_added"):
        format_csv_row_agg.header_added = False

    # Initialize a counter for the new index (could also be done globally, or managed externally)
    if not hasattr(format_csv_row_agg, "index"):
        format_csv_row_agg.index = 0

    # Prepare the CSV output
    csv_output = []

    # Add the column names only if they haven't been added yet
    if not format_csv_row_agg.header_added:
        csv_output.append("index,Month,Average_Weekly_Sales")
        format_csv_row_agg.header_added = True

    # Create a row in CSV format
    row = f"{format_csv_row_agg.index},{record['Month']},{record['Average_Weekly_Sales']}"
    csv_output.append(row)

    # Increment the index for the next record
    format_csv_row_agg.index += 1

    # Return the CSV row as a string, joined by newline characters
    return "\n".join(csv_output)    

# Function to save CSV
def save_to_csv(records, path):
    with open(path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

def validate_paths(path1, path2):
    """Check if both paths exist locally. Returns True if both exist, else False."""
    if os.path.exists(path1) and os.path.exists(path2):
        return True

# Define the Dataflow Pipeline
def run_pipeline():
    pipeline_options = PipelineOptions(
        runner='DirectRunner'  # Run locally
    )

    with beam.Pipeline(options=pipeline_options) as p:
         
        # Read and parse CSV
        csv_pcoll = (
            p
            | 'Read CSV' >> beam.io.ReadFromText('/Users/nicolas/repos/walmart/data/grocery_sales.csv', skip_header_lines=1)
            | 'Parse CSV and Fill Nulls' >> beam.ParDo(ParseDataDoFn())  # Use the consolidated parsing function
        )        
        
        # Read and parse Parquet
        parquet_pcoll = (
            p
            | 'Read Parquet' >> beam.io.ReadFromParquet('/Users/nicolas/repos/walmart/data/extra_data.parquet')
            | 'Parse Parquet' >> beam.Map(parse_parquet)
            | 'Key By Index (Parquet)' >> beam.Map(lambda x: (x['index'], x))
        )

         # Key CSV by 'index' for merging
        keyed_csv_pcoll = csv_pcoll | 'Key By Index (CSV)' >> beam.Map(lambda x: (x['index'], x))


        # Merge CSV and Parquet on 'index'
        merged_pcoll = (
            {'csv': keyed_csv_pcoll, 'parquet': parquet_pcoll}
            | 'Merge on index' >> beam.CoGroupByKey()
            | 'Flatten Merged Data' >> beam.Map(merge_records)
            | 'Filter Out Empty Merged Records' >> beam.Filter(lambda x: x is not None)  # Remove None records
        )
       
        # Extract Month and Filter by Sales
        filtered_pcoll = (
            merged_pcoll
            | 'Extract Month' >> beam.Map(extract_month)
            | 'Filter Sales' >> beam.Filter(filter_sales)
        )
        
        # Select relevant columns
        clean_pcoll = filtered_pcoll | 'Select Columns' >> beam.Map(select_columns)
        #debug_pcoll = clean_pcoll | 'Inspect Records' >> beam.Map(lambda record: print(f"Debugging record: {record}") or record)

        
        # Calculate aggregated sales
        aggregated_pcoll = (
            clean_pcoll
            | 'Group By Month' >> beam.GroupBy(lambda x: x['Month'])
            | 'Calculate Average Sales' >> beam.Map(calculate_average_sales)
        )
        
        # Write clean PCollection to CSV
        formatted_clean_pcoll = clean_pcoll | 'Format to CSV' >> beam.Map(format_csv_row_clean)  # Convert each dictionary to CSV row
        formatted_clean_pcoll | 'Write Clean CSV' >> beam.io.WriteToText('/Users/nicolas/repos/walmart/data/csv_clean_dataflow', file_name_suffix='.csv', shard_name_template='')
        
        # Write aggregated PCollection to CSV
        formatted_agg_pcoll = aggregated_pcoll | 'Format to Agg CSV' >> beam.Map(format_csv_row_agg)  # Convert each dictionary to CSV row
        formatted_agg_pcoll | 'Write Aggregated CSV' >> beam.io.WriteToText('/Users/nicolas/repos/walmart/data/csv_aggregated_dataflow', file_name_suffix='.csv', shard_name_template='')

    path1 = '/Users/nicolas/repos/walmart/data/csv_clean_dataflow.csv'
    path2 = '/Users/nicolas/repos/walmart/data/csv_aggregated_dataflow.csv'

    if validate_paths(path1, path2):
        print("Both paths exist.")
    else:
        print("One or both paths do not exist.")

if __name__ == '__main__':
    run_pipeline()