import apache_beam as beam
import csv
from io import StringIO
import logging
import pandas as pd


# Define a DoFn class that handles both parsing and filling "null" values for CSV
class ParseCSVTransform(beam.DoFn):
    def __init__(self):
        self.last_valid_values = {}

    def process(self, row):
        row = self._parse_csv_line(row)

        # Convert and validate Store_ID
        try:
            row["index"] = int(row["index"])  # Convert index to integer
            row["Store_ID"] = int(row["Store_ID"])  # Convert Store_ID to integer
            if row["Store_ID"] not in [1, 2]:
                return  # Skip invalid store IDs
        except (ValueError, TypeError):
            return

        # Forward-fill values
        row = self._fill_missing_values(row)

        # Only yield if we have required values
        if row.get("Date") and row.get("Weekly_Sales"):
            yield row

    def _parse_csv_line(self, line):
        """Helper function to parse a CSV line."""
        reader = csv.DictReader(
            StringIO(line),
            fieldnames=["index", "Store_ID", "Date", "Dept", "Weekly_Sales"],
        )
        return next(reader)

    def _fill_missing_values(self, row):
        store_id = row["Store_ID"]

        # Fill missing 'Weekly_Sales'
        row["Weekly_Sales"] = self._fill_with_last_valid(
            "Weekly_Sales", row.get("Weekly_Sales"), converter=float
        )
        # Handle missing or 'null' dates
        row["Date"] = self._fill_with_last_valid(
            "Date", row.get("Date"), converter=pd.to_datetime
        )

        return row

    def _fill_with_last_valid(self, key, value, converter=None):
        if value == "null" or value == "None" or value is None or value == "":
            # If missing, use the last valid value
            return self.last_valid_values.get(key)
        else:
            try:
                # Convert the value if it's valid and update last valid values
                if converter:
                    value = converter(value)
                self.last_valid_values[key] = value
                return value
            except Exception as e:
                logging.error(f"Error converting value '{value}' for key '{key}': {e}")
                return self.last_valid_values.get(key)


class ParseParquetTransform(beam.DoFn):
    def __init__(self):
        self.last_valid_values = {}

    def process(self, row):
        row = dict(row)
        try:
            row["index"] = int(row["index"])  # Convert index to integer
        except (ValueError, TypeError):
            return

        row = self._fill_missing_values(row)
        yield row

    def _fill_missing_values(self, row):
        for field in ["CPI", "Unemployment"]:
            row[field] = self._fill_with_last_valid(
                field, row.get(field), converter=float
            )
        return row

    def _fill_with_last_valid(self, key, value, converter=None):
        if value == "null" or value == "None" or value is None:
            value = self.last_valid_values.get(key)
        else:
            try:
                value = converter(value)
                self.last_valid_values[key] = value
            except Exception as e:
                logging.error(f"Error converting value '{value}' for key '{key}': {e}")
                value = self.last_valid_values.get(key)
        return value
