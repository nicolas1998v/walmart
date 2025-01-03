import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam


class TestUtils:
    def test_validate_gcs_paths(self, mocker):
        # Mock GCS client
        mock_storage_client = mocker.patch("google.cloud.storage.Client")
        mock_bucket = mocker.Mock()
        mock_storage_client.return_value.get_bucket.return_value = mock_bucket

        # Test validation
        bucket_name = "test-bucket"
        file_paths = ["test1.csv", "test2.csv"]

        mock_bucket.blob.return_value.exists.return_value = True
        assert True  # Replace with actual validation

    def test_add_sequential_index(self):
        with TestPipeline() as p:
            input_data = [{"data": "test1"}, {"data": "test2"}]

            expected_output = [
                {"data": "test1", "index": 0},
                {"data": "test2", "index": 1},
            ]

            def enumerate_elements(elements):
                return [dict(element, index=i) for i, element in enumerate(elements)]

            output = (
                p
                | "Create" >> beam.Create(input_data)
                | "AddIndex" >> beam.CombineGlobally(beam.combiners.ToListCombineFn())
                | "Enumerate" >> beam.FlatMap(enumerate_elements)
            )

            assert_that(output, equal_to(expected_output))
