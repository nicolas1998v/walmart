from google.cloud import storage
import apache_beam as beam
import logging

def validate_gcs_paths(bucket_name, paths):
    """Validate that files exist in GCS bucket"""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    
    for path in paths:
        blob_name = path.replace(f'gs://{bucket_name}/', '')
        blob = bucket.blob(blob_name)
        exists = blob.exists()
        logging.info(f"Checking {blob_name}: {'exists' if exists else 'does not exist'}")
        if not exists:
            return False
    return True

def round_floats(record):
    """Round all float values in a dictionary to 2 decimal places"""
    return {k: round(v, 2) if isinstance(v, float) else v for k, v in record.items()}

class AddSequentialIndex(beam.DoFn):
    """Apache Beam DoFn that adds a sequential index to each element in the PCollection.
"""
    def setup(self):
        # Initialize counter when the DoFn starts
        self.counter = 0
    
    def start_bundle(self):
        # Reset counter at the start of each bundle
        self.counter = 0
    
    def process(self, element):
        element['index'] = self.counter
        self.counter += 1
        yield element