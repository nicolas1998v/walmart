I've decided to build a data cleaning and aggregation pipeline using walmart data in a CSV and local economic data in a Parquet file. I've done the same pipeline four times to showcase my skills in these four technologies. This pipeline provides two output files.

The technologies are: Python Pandas, Spark DataFrames, Spark RDD's and Apache Beam Pcollections with GCP and Dataflow. Here is a breakdown of what this pipeline does:

Pipeline Steps:
1. Import data from SQLite, CSV and parquet sources
2. Merge datasets on index
3. Transform data (clean nulls, filter sales, select columns)
4. Calculate monthly sales averages
5. Export two CSV files. A clean_output.csv and an aggregated_output.csv with these monthly sales averages
6. Validate we have both output files locally or in the cloud

## Testing

The project includes comprehensive test coverage:
- Unit tests for all transforms
- Integration tests for pipeline components
- Performance tests for data processing
- Type hint validation
- Documentation coverage

Run tests with bash:
pytest tests/

