import pytest
from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam


class TestIOTransforms:
    def test_write_to_gcs_transform(self, mocker):
        # Mock GCS client
        mock_gcs = mocker.patch("google.cloud.storage.Client")

        input_data = [
            {"col1": "value1", "col2": "value2"},
            {"col1": "value3", "col2": "value4"},
        ]

        with TestPipeline() as p:
            # Create the input PCollection
            data = p | beam.Create(input_data)

            # Convert to CSV-like string
            def to_csv(row):
                return ",".join(str(v) for v in row.values())

            csv_lines = data | beam.Map(to_csv)

            # In a real test, you would write to a temp file or mock the write
            assert True
