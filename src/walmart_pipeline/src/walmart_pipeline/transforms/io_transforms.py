import apache_beam as beam
import logging
from .utils import AddSequentialIndex   

class WriteToGCSTransform(beam.PTransform):
    """A PTransform for writing dictionaries to GCS as CSV files.

    This transform takes a PCollection of dictionaries, adds sequential indices,
    converts them to CSV format, and writes them to Google Cloud Storage."""

    def __init__(self, output_path, headers):
        self.output_path = output_path
        self.headers = headers

    def expand(self, pcoll):
                
        return (
            pcoll
            | 'Add Index' >> beam.ParDo(AddSequentialIndex())
            | 'Convert to CSV' >> beam.Map(self.dict_to_csv)
            | 'Write to GCS' >> beam.io.WriteToText(
                self.output_path,
                header=self.headers,
                shard_name_template=''  # Single output file
            )
        )


    @staticmethod
    def dict_to_csv(row):
        """Converts a dictionary row to CSV format string for output files."""
        # Create a copy without modifying the original
        row_copy = dict(row)
        # Get the index value first
        index = str(row_copy.get('index', ''))
        # Remove index from the copy to handle remaining fields
        row_copy.pop('index', None)
        # Get ordered values for remaining fields
        ordered_values = [str(row_copy.get(field, '')) for field in row_copy.keys()]
        # Return CSV string with index as first column
        return ','.join([index] + ordered_values)
