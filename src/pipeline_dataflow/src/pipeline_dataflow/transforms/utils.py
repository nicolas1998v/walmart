from google.cloud import storage
import apache_beam as beam
import logging


def validate_gcs_paths(bucket_name, paths):
    """Validate that files exist in GCS bucket"""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    for path in paths:
        blob_name = path.replace(f"gs://{bucket_name}/", "")
        blob = bucket.blob(blob_name)
        exists = blob.exists()
        logging.info(
            f"Checking {blob_name}: {'exists' if exists else 'does not exist'}"
        )
        if not exists:
            return False
    return True


class RoundFloatsDoFn(beam.DoFn):
    """DoFn to round float values in a record to 2 decimal places"""

    def process(self, record):
        if not isinstance(record, dict):
            yield record
            return

        rounded = {}
        for key, value in record.items():
            if isinstance(value, float):
                rounded[key] = round(value, 2)
            else:
                rounded[key] = value
        yield rounded


class AddSequentialIndex(beam.DoFn):
    def __init__(self):
        self._counter = 0
    
    def start_bundle(self):
        self._counter = 0

    def process(self, element):
        self._counter += 1
        element['index'] = self._counter  # Simply replace the index with counter
        yield element