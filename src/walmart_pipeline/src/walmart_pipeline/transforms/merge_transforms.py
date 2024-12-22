import apache_beam as beam
import logging

class MergeTransform(beam.PTransform):
    """Apache Beam PTransform that merges records from CSV and Parquet sources.
    
    This transform performs a CoGroupByKey operation to join records from two sources
    based on a common index, then combines them into a single record containing
    sales data from CSV and economic indicators from Parquet.
    """
    def expand(self, pcoll_dict):
        return (pcoll_dict 
                | 'Merge on Index' >> beam.CoGroupByKey()
                | 'Combine Records' >> beam.Map(self.merge_records)
                | 'Filter Valid' >> beam.Filter(lambda x: x is not None))
    
    @staticmethod
    def merge_records(element):
        key, grouped_records = element
        
        # Get CSV records - if none exist, skip this record
        csv_records = grouped_records.get('csv', [])
        if not csv_records:
            return None
            
        # Get matching parquet record if it exists
        parquet_records = grouped_records.get('parquet', [])
        
        try:
            csv_record = csv_records[0]
            parquet_record = parquet_records[0] if parquet_records else {}
            
            # Merge records
            merged = {
                'index': key,
                'Store_ID': csv_record.get('Store_ID'),
                'Date': csv_record.get('Date'),
                'Dept': csv_record.get('Dept'),
                'Weekly_Sales': csv_record.get('Weekly_Sales'),
                'CPI': parquet_record.get('CPI', 0.0),  # Default to 0.0 if no parquet record
                'Unemployment': parquet_record.get('Unemployment', 0.0)  # Default to 0.0 if no parquet record
            }
            
            logging.info(f"Successfully merged record for index {key}")
            return merged
            
        except Exception as e:
            logging.error(f"Error merging records for key {key}: {e}")
            return None