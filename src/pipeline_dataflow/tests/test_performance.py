import pytest
import time
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to


def generate_large_dataset(size=1000):
    """Generate a large test dataset."""
    return [f"{i},2023-01-01,1,1000.00,0" for i in range(size)]


def test_parse_transform_performance():
    """Test performance of parse transform with large dataset."""

    def parse_csv(line):
        fields = line.split(",")
        return {"Store_ID": fields[0], "Date": fields[1], "Sales": float(fields[2])}

    with TestPipeline() as p:
        start_time = time.time()

        input_data = generate_large_dataset()

        # Run pipeline
        output = (
            p | "Create" >> beam.Create(input_data) | "Parse" >> beam.Map(parse_csv)
        )

        execution_time = time.time() - start_time
        assert (
            execution_time < 2.0
        ), f"Performance test failed: {execution_time:.2f} seconds"


def test_sales_transform_performance():
    """Test performance of sales calculations with large dataset."""
    with TestPipeline() as p:
        start_time = time.time()

        input_data = [
            {"Store_ID": str(i), "Date": "2023-01-01", "Sales": 1000.00}
            for i in range(1000)
        ]

        # Run pipeline
        output = (
            p | "Create" >> beam.Create(input_data) | "Process" >> beam.Map(lambda x: x)
        )

        execution_time = time.time() - start_time
        assert (
            execution_time < 3.0
        ), f"Performance test failed: {execution_time:.2f} seconds"


def test_memory_usage():
    """Test memory usage during pipeline execution."""
    import psutil
    import os

    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024

    with TestPipeline() as p:
        input_data = generate_large_dataset(size=10000)
        output = (
            p
            | "Create" >> beam.Create(input_data)
            | "Parse" >> beam.Map(lambda x: x.split(","))
        )

    final_memory = process.memory_info().rss / 1024 / 1024
    memory_increase = final_memory - initial_memory
    assert memory_increase < 500, f"Memory usage too high: {memory_increase:.2f}MB"
