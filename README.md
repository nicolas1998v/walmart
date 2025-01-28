I've decided to build a data cleaning and aggregation pipeline using walmart data in a CSV and local economic data in a Parquet file. I've done the same pipeline six times to showcase my skills in these six technologies. This pipeline provides two output files.

The technologies are: Python Pandas, Spark DataFrames, Spark RDD's, Apache Beam Pcollections with GCP and Dataflow, Flink and Airflow. Here is a breakdown of what this pipeline does:


Pipeline Steps:
1. Import data from SQLite, CSV and parquet sources
2. Merge datasets on index
3. Transform data (clean nulls, filter sales, select columns)
4. Calculate monthly sales averages
5. Export two CSV files. A clean_output.csv and an aggregated_output.csv with these monthly sales averages
6. Validate we have both output files locally or in the cloud

## Executing

To run the code, clone the repo, install the requirements and run : 

```sh
python pipeline_spark_rdd.py
python pipeline_spark_dataframes.py
python pipeline_pandas.py
cd pipeline_dataflow/src && python -m pipeline_dataflow.main
python pipeline_flink.py
```
For the airfloe script, download the latest airflow package, create an account, create a GCP connection and then upload the dag and trigger it.

## Testing

The project includes comprehensive test coverage:


| Test Name | Description |
|-----------|-------------|
| test_run_pipeline_cloud | Verifies pipeline runs with Dataflow runner and cloud options |
| test_parse_transform_performance | Checks if ParseCSVTransform can handle large datasets efficiently |
| test_parse_csv_transform_with_invalid_data | Checks how parser handles malformed CSV data |
| test_parse_csv_transform_with_missing_values | Verifies parser's handling of CSV rows with missing fields |
| test_sales_transform_performance | Verifies SalesTransform's performance with large datasets |
| test_memory_usage | Monitors memory usage during pipeline operations |
| test_write_to_gcs_transform | Verifies data is correctly written to Google Cloud Storage |
| test_merge_transform | Tests if data from multiple sources is merged correctly |
| test_sales_transform | Tests the business logic for sales data aggregation |
| test_validate_gcs_paths | Verifies GCS path validation functionality |
| test_add_sequential_index | Tests if sequential indices are correctly added to records |

Run tests with bash:
```sh
pytest pipeline_dataflow/pipeline_dataflow/tests/
```

<img width="1440" alt="image" src="https://github.com/user-attachments/assets/381b4b25-d613-4ca8-9504-9304f3113707" />




