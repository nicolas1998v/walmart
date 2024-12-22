import apache_beam as beam
import logging
import csv
from io import StringIO
import pandas as pd

class ParseCSVTransform(beam.PTransform):

    """Apache Beam transform for parsing Walmart sales CSV data.
    - Parses raw CSV lines into structured dictionaries
    - Fills missing values using last known valid values
    - Converts Weekly_Sales to float and Date to datetime
    - Logs errors for problematic rows
    - Maintains state of last valid values for imputation
    """

    def expand(self, pcoll):
        return pcoll | beam.ParDo(ParseCSVDoFn())

class ParseCSVDoFn(beam.DoFn):
    def __init__(self):
        self.last_valid_values = {}

    def process(self, row):
        try:
            row = self._parse_csv_line(row)
            row = self._fill_missing_values(row)
            yield row
        except Exception as e:
            logging.error(f"Error parsing row: {row}. Error: {e}")

    def _parse_csv_line(self, line):
        reader = csv.DictReader(
            StringIO(line), 
            fieldnames=['index', 'Store_ID', 'Date', 'Dept', 'Weekly_Sales']
        )
        return next(reader)

    def _fill_missing_values(self, row):
        row['Weekly_Sales'] = self._fill_with_last_valid('Weekly_Sales', row.get('Weekly_Sales'), converter=float)
        row['Date'] = self._fill_with_last_valid('Date', row.get('Date'), converter=pd.to_datetime)
        return row

    def _fill_with_last_valid(self, key, value, converter=None):
        if value in ('null', 'None', None):
            last_value = self.last_valid_values.get(key)
            logging.info(f"Filling {key} with last valid value: {last_value} (was {value})")
            return last_value
        try:
            value = converter(value)
            self.last_valid_values[key] = value
            return value
        except Exception as e:
            logging.error(f"Error converting value '{value}' for key '{key}': {e}")
            return 0.0

class ParseParquetTransform(beam.PTransform):

    """Apache Beam transform processing individual rows from Parquet economic data.
    Handles data cleaning with the following features:
    - Converts index to string format
    - Fills missing values for Unemployment and CPI fields
    - Maintains last valid values for each economic indicator
    - Handles type conversion errors gracefully
    """
    
    def expand(self, pcoll):
        return pcoll | beam.ParDo(ParseParquetDoFn())

class ParseParquetDoFn(beam.DoFn):
    def __init__(self):
        self.last_valid_values = {}

    def process(self, row):
        row = dict(row)
        row['index'] = str(row['index'])
        row = self._fill_missing_values(row)
        yield row

    def _fill_missing_values(self, row):
        for field in ['Unemployment', 'CPI']:
            row[field] = self._fill_with_last_valid(field, row.get(field), converter=float)
        return row

    def _fill_with_last_valid(self, key, value, converter=None):
        if value in ('null', 'None', None):
            value = self.last_valid_values.get(key)
        try:
            value = converter(value)
            self.last_valid_values[key] = value
        except Exception as e:
            logging.error(f"Error converting value '{value}' for key '{key}': {e}")
            value = None
        return value