import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline_dataflow.src.pipeline_dataflow.transforms.merge_transforms import (
    MergeTransform,
)
import pandas as pd


def check_output(expected):
    """Returns a function that checks if actual output matches expected output."""

    def _check(actual):
        actual = list(actual)
        if len(actual) != len(expected):
            return False

        for act, exp in zip(
            sorted(actual, key=lambda x: x["index"]),
            sorted(expected, key=lambda x: x["index"]),
        ):
            if act.keys() != exp.keys():
                return False
            for key in exp:
                if key == "Date":
                    if pd.Timestamp(act[key]) != pd.Timestamp(exp[key]):
                        return False
                elif isinstance(exp[key], float):
                    if abs(act[key] - exp[key]) > 0.001:
                        return False
                else:
                    if act[key] != exp[key]:
                        return False
        return True

    return _check


class TestMergeTransforms:
    def test_merge_transform(self):
        """Test basic merge functionality."""
        with TestPipeline() as p:
            # Sample sales data
            sales_data = [
                (
                    "0",
                    {
                        "Store_ID": "1",
                        "Date": pd.Timestamp("2023-01-01"),
                        "Weekly_Sales": 100.0,
                        "Dept": "1",
                    },
                ),
                (
                    "1",
                    {
                        "Store_ID": "2",
                        "Date": pd.Timestamp("2023-01-01"),
                        "Weekly_Sales": 200.0,
                        "Dept": "2",
                    },
                ),
            ]

            # Sample economic data
            economic_data = [
                ("0", {"Unemployment": 5.0, "CPI": 100.0}),
                ("1", {"Unemployment": 5.0, "CPI": 100.0}),
            ]

            # Create PCollections
            sales_pcoll = p | "Create Sales" >> beam.Create(sales_data)
            economic_pcoll = p | "Create Economic" >> beam.Create(economic_data)

            # Apply merge transform
            merged = {
                "csv": sales_pcoll,
                "parquet": economic_pcoll,
            } | "Merge" >> MergeTransform()

            expected_output = [
                {
                    "index": "0",
                    "Store_ID": "1",
                    "Date": pd.Timestamp("2023-01-01"),
                    "Dept": "1",
                    "Weekly_Sales": 100.0,
                    "CPI": 100.0,
                    "Unemployment": 5.0,
                },
                {
                    "index": "1",
                    "Store_ID": "2",
                    "Date": pd.Timestamp("2023-01-01"),
                    "Dept": "2",
                    "Weekly_Sales": 200.0,
                    "CPI": 100.0,
                    "Unemployment": 5.0,
                },
            ]

            assert_that(merged, check_output(expected_output))
