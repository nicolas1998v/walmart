import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import pandas as pd

from pipeline_dataflow.src.pipeline_dataflow.transforms.parse_transforms import (
    ParseCSVTransform,
    ParseParquetTransform,
)


def test_parse_csv_transform_with_invalid_data():
    with TestPipeline() as p:
        input_data = [
            "1,1,2023-01-01,1,1000.5",
            "2,invalid,2023-01-01,1,1500.25",
            "3,2,2023-01-01,1,1500.25",
            "invalid,data,row,here,now",
        ]

        output = p | beam.Create(input_data) | beam.ParDo(ParseCSVTransform())

        # Debug: Print actual output
        output | beam.Map(print)  # Add this line

    expected_output = [
        {
            "index": 1,  # Integer
            "Store_ID": 1,  # Integer
            "Date": pd.Timestamp("2023-01-01"),
            "Dept": "1",  # Keep as string
            "Weekly_Sales": 1000.5,
        },
        {
            "index": 3,  # Integer
            "Store_ID": 2,  # Integer
            "Date": pd.Timestamp("2023-01-01"),
            "Dept": "1",  # Keep as string
            "Weekly_Sales": 1500.25,
        },
    ]

    assert_that(output, equal_to(expected_output))


def test_parse_csv_transform_with_missing_values():
    with TestPipeline() as p:
        input_data = [
            "1,1,2023-01-01,1,1000.50",
            "2,1,,1,2000.75",  # Missing date
            "3,1,2023-01-03,1,",  # Missing sales
        ]

        output = p | beam.Create(input_data) | beam.ParDo(ParseCSVTransform())

        expected_output = [
            {
                "index": 1,
                "Store_ID": 1,
                "Date": pd.Timestamp("2023-01-01"),
                "Dept": "1",
                "Weekly_Sales": 1000.5,
            },
            {
                "index": 2,
                "Store_ID": 1,
                "Date": pd.Timestamp("2023-01-01"),  # Forward-filled from previous
                "Dept": "1",
                "Weekly_Sales": 2000.75,
            },
            {
                "index": 3,
                "Store_ID": 1,
                "Date": pd.Timestamp("2023-01-03"),
                "Dept": "1",
                "Weekly_Sales": 2000.75,  # Forward-filled from previous
            },
        ]

        assert_that(output, equal_to(expected_output))


def test_parse_parquet_transform():
    with TestPipeline() as p:
        input_data = [
            {
                "index": "1",
                "Store_ID": 1,
                "CPI": 100.0,
                "Unemployment": 5.0,
                "Type": "A",
                "Size": 100000,
                "Temperature": 75.5,
                "Fuel_Price": 3.5,
                "Markdown1": 0,
                "Markdown2": 0,
                "Markdown3": 0,
                "Markdown4": 0,
                "Markdown5": 0,
                "IsHoliday": False,
            },
            {
                "index": "2",
                "Store_ID": 1,
                "CPI": None,
                "Unemployment": 6.0,
                "Type": "A",
                "Size": 100000,
                "Temperature": 76.0,
                "Fuel_Price": 3.55,
                "Markdown1": 0,
                "Markdown2": 0,
                "Markdown3": 0,
                "Markdown4": 0,
                "Markdown5": 0,
                "IsHoliday": False,
            },
            {
                "index": "3",
                "Store_ID": 2,
                "CPI": 102.0,
                "Unemployment": None,
                "Type": "B",
                "Size": 90000,
                "Temperature": 74.5,
                "Fuel_Price": 3.45,
                "Markdown1": 0,
                "Markdown2": 0,
                "Markdown3": 0,
                "Markdown4": 0,
                "Markdown5": 0,
                "IsHoliday": True,
            },
        ]

        output = p | beam.Create(input_data) | beam.ParDo(ParseParquetTransform())

        expected_output = [
            {
                "index": 1,  # Convert to integer
                "Store_ID": 1,  # Already integer
                "CPI": 100.0,
                "Unemployment": 5.0,
                "Type": "A",
                "Size": 100000,
                "Temperature": 75.5,
                "Fuel_Price": 3.5,
                "Markdown1": 0,
                "Markdown2": 0,
                "Markdown3": 0,
                "Markdown4": 0,
                "Markdown5": 0,
                "IsHoliday": False,
            },
            {
                "index": 2,
                "Store_ID": 1,
                "CPI": 100.0,  # Forward-filled from previous
                "Unemployment": 6.0,
                "Type": "A",
                "Size": 100000,
                "Temperature": 76.0,
                "Fuel_Price": 3.55,
                "Markdown1": 0,
                "Markdown2": 0,
                "Markdown3": 0,
                "Markdown4": 0,
                "Markdown5": 0,
                "IsHoliday": False,
            },
            {
                "index": 3,
                "Store_ID": 2,
                "CPI": 102.0,
                "Unemployment": 6.0,  # Forward-filled from previous
                "Type": "B",
                "Size": 90000,
                "Temperature": 74.5,
                "Fuel_Price": 3.45,
                "Markdown1": 0,
                "Markdown2": 0,
                "Markdown3": 0,
                "Markdown4": 0,
                "Markdown5": 0,
                "IsHoliday": True,
            },
        ]

        assert_that(output, equal_to(expected_output))
