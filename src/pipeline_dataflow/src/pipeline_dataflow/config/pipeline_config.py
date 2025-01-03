from dataclasses import dataclass

'''
    """Configuration class for the Walmart data pipeline.
    
    This class manages all the configuration parameters needed for the data pipeline,
    including GCP project details, storage locations, and BigQuery table references.
    
'''


@dataclass
class PipelineConfig:
    project_id: str = "nico-playground-384514"
    bucket_name: str = "walmart-7890837"
    region: str = "us-east1"

    @property
    def temp_location(self):
        return f"gs://{self.bucket_name}/temp"

    @property
    def input_csv(self):
        return f"gs://{self.bucket_name}/grocery_sales.csv"

    @property
    def input_parquet(self):
        return f"gs://{self.bucket_name}/extra_data.parquet"

    @property
    def clean_output(self):
        return f"gs://{self.bucket_name}/clean_output.csv"

    @property
    def agg_output(self):
        return f"gs://{self.bucket_name}/aggregated_output.csv"

    @property
    def clean_table(self):
        return f"{self.project_id}:clean_walmart.clean"

    @property
    def agg_table(self):
        return f"{self.project_id}:agg_walmart.aggregated"
