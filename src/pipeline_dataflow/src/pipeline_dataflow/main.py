import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    GoogleCloudOptions,
)
import logging
import os
import pandas as pd
from pipeline_dataflow.src.pipeline_dataflow.transforms.parse_transforms import (
    ParseCSVTransform,
    ParseParquetTransform,
)
from pipeline_dataflow.src.pipeline_dataflow.transforms.merge_transforms import (
    MergeTransform,
)
from pipeline_dataflow.src.pipeline_dataflow.transforms.sales_transforms import (
    SalesTransform,
)
from pipeline_dataflow.src.pipeline_dataflow.schemas import (
    CLEAN_SCHEMA,
    AGG_SCHEMA,
    CLEAN_CSV_HEADERS,
    AGG_CSV_HEADERS,
)
from pipeline_dataflow.src.pipeline_dataflow.transforms.utils import (
    validate_gcs_paths,
    RoundFloatsDoFn,
    AddSequentialIndex,
)


def run_pipeline(options=None):
    """
    Executes a Dataflow pipeline that processes Walmart sales data.

    The pipeline performs the following operations:
    1. Reads and parses data from CSV and Parquet files
    2. Merges and cleans the data
    3. Writes clean data to BigQuery and GCS
    4. Aggregates sales data
    5. Writes aggregated data to BigQuery and GCS
    6. Validates output files in GCS
    """
    # Setup configuration for the pipeline
    setup_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "setup.py")
    )

    if options is None:
        options = PipelineOptions(
            project="nico-playground-384514",
            temp_location="gs://walmart-7890837/temp",
            region="us-east1",
            runner="DataflowRunner",
            setup_file=setup_file,  # Add setup file
            save_main_session=True,  # Save main session
        )
    # Set additional options
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    # Get Google Cloud options for BigQuery
    gcloud_options = options.view_as(GoogleCloudOptions)

    with beam.Pipeline(options=options) as p:

        # Stage 1: Data Ingestion
        # Process CSV data: read, parse, and prepare for merging
        csv_data = (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                "gs://walmart-7890837/grocery_sales.csv", skip_header_lines=1
            )
            | "Parse CSV" >> beam.ParDo(ParseCSVTransform())
            | "Log CSV"
            >> beam.Map(
                lambda x: (logging.info(f"CSV Record: {x}"), x)[1]
            )  # Add logging
            | "Key CSV by Index" >> beam.Map(lambda x: (x["index"], x))
        )

        # Process Parquet data: read, parse, and prepare for merging
        parquet_data = (
            p
            | "Read Parquet"
            >> beam.io.ReadFromParquet("gs://walmart-7890837/extra_data.parquet")
            | "Parse Parquet" >> beam.ParDo(ParseParquetTransform())
            | "Log Parquet"
            >> beam.Map(
                lambda x: (logging.info(f"Parquet Record: {x}"), x)[1]
            )  # Add logging
            | "Key Parquet by Index" >> beam.Map(lambda x: (x["index"], x))
        )

        # Stage 2: Data Cleaning and Transformation
        # Merge CSV and Parquet data, clean and standardize data types
        clean_data = (
            {"csv": csv_data, "parquet": parquet_data}
            | "Merge Data" >> MergeTransform()
            | "Count Records"
            >> beam.Map(
                lambda x: (logging.info(f"Processing record: {x['index']}"), x)[1]
            )
            | "Select Columns"
            >> beam.Map(
                lambda x: {
                    "Store_ID": int(x["Store_ID"]),  # Ensure integer
                    "Month": int((x["Date"]).month),  # Ensure integer
                    "Dept": int(x["Dept"]),  # Ensure integer
                    "IsHoliday": int(x.get("IsHoliday", 0)),  # Ensure boolean
                    "Weekly_Sales": float(x["Weekly_Sales"]),  # Ensure float
                    "CPI": float(x.get("CPI", 0.0)),  # Ensure float
                    "Unemployment": float(x.get("Unemployment", 0.0)),  # Ensure float
                }
            )
        )

        # Stage 3: Data Output
        # Write clean data to BigQuery and GCS
        _ = (
            clean_data
            | "Round Clean Data" >> beam.ParDo(RoundFloatsDoFn())  # Add rounding
            | "Write Clean to BQ"
            >> beam.io.WriteToBigQuery(
                "nico-playground-384514:clean_walmart.clean",
                schema=CLEAN_SCHEMA,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                method="FILE_LOADS",
                temp_file_format="AVRO",
                custom_gcs_temp_location=gcloud_options.temp_location,
            )
        )

        def format_clean_csv(element):
            """Format clean data for CSV output, with a clean index."""
            logging.info(f"Element received in format_clean_csv: {element}")
            index, record = element  # Unpack the tuple
            return f"{index},{record['Store_ID']},{record['Month']},{record['Dept']},{record.get('IsHoliday', '')},{record['Weekly_Sales']},{record.get('CPI', '')},{record.get('Unemployment', '')}"

        clean_with_index = (
            clean_data
            | "Reset Index"
            >> beam.Map(
                lambda x: {k: x[k] for k in x if k != "index"}
            )  # Remove any existing index
            | "Add Sequential Index" >> beam.ParDo(AddSequentialIndex())
            | "Log After Index"
            >> beam.Map(lambda x: (logging.info(f"After index: {x}"), x)[1])
            | "Format Clean CSV" >> beam.Map(format_clean_csv)
        )
        _ = (
            (
                p | "Create Clean Header" >> beam.Create([CLEAN_CSV_HEADERS]),
                clean_with_index,
            )
            | "Flatten Clean Data" >> beam.Flatten()
            | "Write Clean to GCS"
            >> beam.io.WriteToText(
                "gs://walmart-7890837/clean_output.csv", shard_name_template=""
            )
        )

        # Stage 4: Data Aggregation and Final Output
        # Process sales data for aggregated metrics
        sales_data = clean_data | "Process Sales" >> SalesTransform()

        # Write aggregated data to BigQuery
        _ = (
            sales_data
            | "Round Agg Data" >> beam.ParDo(RoundFloatsDoFn())  # Add rounding
            | "Write Agg to BQ"
            >> beam.io.WriteToBigQuery(
                "nico-playground-384514:agg_walmart.aggregated",
                schema=AGG_SCHEMA,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                method="FILE_LOADS",
                temp_file_format="AVRO",
                custom_gcs_temp_location=gcloud_options.temp_location,
            )
        )

        def format_agg_csv(element):
            """Format aggregated data for CSV output, with month-based index."""
            index, record = element  # Unpack the tuple
            return f"{index},{record['Month']},{record['Average_Weekly_Sales']}"

        agg_with_index = (
            sales_data
            | "Add Sequential Index to Agg" >> beam.ParDo(AddSequentialIndex())
            | "Format Agg CSV" >> beam.Map(format_agg_csv)
        )

        _ = (
            (p | "Create Agg Header" >> beam.Create([AGG_CSV_HEADERS]), agg_with_index)
            | "Flatten Agg Data" >> beam.Flatten()
            | "Write Agg to GCS"
            >> beam.io.WriteToText(
                "gs://walmart-7890837/agg_output.csv", shard_name_template=""
            )
        )
        # Stage 5: Output Validation
    bucket_name = "walmart-7890837"
    validation_result = validate_gcs_paths(
        bucket_name, ["clean_output.csv", "aggregated_output.csv"]
    )
    logging.info(f"Output validation result: {validation_result}")

    # Run the pipeline and return the result
    return p.run()


if __name__ == "__main__":
    # Configure logging and execute pipeline
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
