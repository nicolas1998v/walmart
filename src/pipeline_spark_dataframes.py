import pyspark as ps
import findspark

findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import to_timestamp, month, round, col, avg, coalesce, last
from pyspark.sql.types import FloatType
import os
import sqlite3
import pandas as pd
from pyspark.sql.window import Window

"""
PySpark ETL Pipeline for Walmart Sales Data Analysis. PySpark Dataframes version. 

This script implements an ETL (Extract, Transform, Load) pipeline using PySpark DF's
to process and analyze Walmart grocery sales data. It combines sales data from
a SQLite database with additional metrics from a Parquet file.

Key functionalities:
- Imports and cleans data from SQLite and Parquet sources
- Merges sales data with supplementary economic indicators
- Transforms data by filtering and calculating monthly averages
- Exports results to CSV files for further analysis

Dependencies:
- PySpark
- findspark
- pandas
- sqlite3

Required files:
- sqlite-jdbc-3.45.2.0.jar: SQLite JDBC driver
- walmart.db: SQLite database containing grocery sales data
- extra_data.parquet: Parquet file with additional metrics
"""

# First, let's ensure the SQLite JDBC driver is available
os.environ["SPARK_CLASSPATH"] = os.path.abspath("./sqlite-jdbc-3.45.2.0.jar")

# Stop existing Spark context if it exists
try:
    sc.stop()
except:
    pass

# Create new SparkContext with SQLite driver
conf = (
    ps.SparkConf()
    .setMaster("local[*]")
    .set("spark.driver.extraClassPath", os.path.abspath("./sqlite-jdbc-3.45.2.0.jar"))
    .set("spark.executor.extraClassPath", os.path.abspath("./sqlite-jdbc-3.45.2.0.jar"))
)

sc = ps.SparkContext(conf=conf)

# Create SparkSession with SQLite configurations
spark = (
    SparkSession.builder.config(conf=conf)
    .config("spark.jars", os.path.abspath("./sqlite-jdbc-3.45.2.0.jar"))
    .getOrCreate()
)


def clean_dataframe(df, is_sales_df=True):
    # Convert datatypes first
    if is_sales_df:
        df = df.withColumn("Date", to_timestamp("Date"))
        df = df.withColumn("Weekly_Sales", df["Weekly_Sales"].cast(FloatType()))
    else:
        df = df.withColumn("Unemployment", df["Unemployment"].cast(FloatType()))

    # Forward fill nulls
    window_spec = Window.orderBy("index")
    for column in df.columns:
        df = df.withColumn(
            column, coalesce(col(column), last(col(column), True).over(window_spec))
        )

    return df


def import_data(db_path, parquet_path):
    conn = sqlite3.connect(db_path)
    pandas_df = pd.read_sql_query("SELECT * FROM grocery_sales", conn)
    conn.close()

    # Convert Pandas DataFrame to Spark DataFrame
    gs_df = spark.createDataFrame(pandas_df)
    extra_df = spark.read.parquet(parquet_path)

    # Clean both dataframes
    gs_df_clean = clean_dataframe(gs_df, is_sales_df=True)
    extra_df_clean = clean_dataframe(extra_df, is_sales_df=False)

    return gs_df_clean, extra_df_clean


def extract(df1, df2):
    merged_df = df1.join(df2, on="index")
    return merged_df


def transform(df):
    df_month = df.withColumn("month", month("Date"))
    df_filtered = df_month.filter(df_month["Weekly_Sales"] > 10000)
    clean_data = df_filtered[
        [
            "Store_ID",
            "month",
            "Dept",
            "IsHoliday",
            "Weekly_Sales",
            "CPI",
            "Unemployment",
        ]
    ]
    return clean_data


def avg_monthly_sales(df):
    # Group by month and calculate average sales
    df_grouped = (
        df.groupby("month")
        .agg(avg("Weekly_Sales").alias("Avg_Sales"))
        .withColumn("Avg_Sales", round(col("Avg_Sales")))
    )
    return df_grouped


def load(df1, df2, path1, path2):
    # Write CSVs with headers and overwrite existing files
    df1.write.mode("overwrite").option("header", True).csv(path1)
    df2.write.mode("overwrite").option("header", True).csv(path2)


def validation(path1, path2):
    if os.path.exists(path1) and os.path.exists(path2):
        return True
    else:
        return False


data_dir = "../data/"
clean_data_path = data_dir + "clean_data_spark_df.csv"
agg_data_path = data_dir + "agg_data_spark_df.csv"

grocery_sales, extra_data = import_data(
    "../data/walmart.db", "../data/extra_data.parquet"
)
merged_df = extract(grocery_sales, extra_data)
clean_data = transform(merged_df)
agg_data = avg_monthly_sales(clean_data)
load(clean_data, agg_data, clean_data_path, agg_data_path)
result = validation(clean_data_path, agg_data_path)
print(f"\nFinal pipeline status: {'Success' if result else 'Failed'}")
