import pytest
from unittest import mock
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineState
from pipeline_dataflow.src.pipeline_dataflow.main import run_pipeline


def test_run_pipeline_cloud():
    pipeline_args = [
        "--input_path=gs://test-bucket/input",
        "--output_path=gs://test-bucket/output",
        "--agg_output_path=gs://test-bucket/agg",
        "--temp_location=gs://test-bucket/temp",
        "--runner=DirectRunner",
        "--project=test-project",
    ]

    options = PipelineOptions(pipeline_args)

    # Create mock pipeline and result
    mock_result = mock.MagicMock()
    mock_result.state = "DONE"  # Use string instead of enum

    mock_pipeline = mock.MagicMock()
    mock_pipeline.__enter__.return_value = mock_pipeline
    mock_pipeline.run.return_value = mock_result

    with mock.patch("apache_beam.Pipeline", return_value=mock_pipeline):
        result = run_pipeline(options)
        assert result is not None
        assert result.state == "DONE"  # Compare with string
        mock_pipeline.run.assert_called_once()
