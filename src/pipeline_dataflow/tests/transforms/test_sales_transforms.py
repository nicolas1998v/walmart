import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam


class TestSalesTransforms:
    def test_sales_transform(self):
        input_data = [
            {
                "Store_ID": 1,
                "Month": 2,
                "Dept": 1,
                "Weekly_Sales": 24924.50,
                "IsHoliday": 0,
                "CPI": 211.1,
                "Unemployment": 8.106,
            }
        ]

        def calculate_sales(record):
            return {
                "Store_ID": record["Store_ID"],
                "Month": record["Month"],
                "Dept": record["Dept"],
                "Avg_Weekly_Sales": record["Weekly_Sales"],
                "Total_Sales": record["Weekly_Sales"],
                "Avg_CPI": record["CPI"],
                "Avg_Unemployment": record["Unemployment"],
            }

        expected_output = [
            {
                "Store_ID": 1,
                "Month": 2,
                "Dept": 1,
                "Avg_Weekly_Sales": 24924.50,
                "Total_Sales": 24924.50,
                "Avg_CPI": 211.1,
                "Avg_Unemployment": 8.106,
            }
        ]

        with TestPipeline() as p:
            output = p | beam.Create(input_data) | beam.Map(calculate_sales)

            assert_that(output, equal_to(expected_output))
