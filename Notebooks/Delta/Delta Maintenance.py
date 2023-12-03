# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import explode, first
from datetime import date
import inspect
import json
import requests


def get_partition_column(table_location=None):
    """
    Returns the column that a table is partitioned by

    Parameters:
    table_location (string): The table schema and table name
    help_flag (bool): If True, prints the docstring of this function

    Examples:
    get_partition_column(table_location = "schema_name.table_name")
    """
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_location}")
        partition_cols = detail.select("partitionColumns").first()[0]
        return partition_cols[0] if partition_cols else None
    except Exception as e:
        print(f"Error retrieving partition column: {e}")
        return None
    
def convert_to_json(data = None):
    return json.dumps(data, indent=4)

def get_delta_stats(
    catalog_name=None, schema_name=None, table_name=None, help_flag=False
):
    """
    Returns a list of metrics related to a delta table. The results can then be analyzed to verify if any maintenance or refactoring is required

    Parameters:
    catalog_name (string): The catalog the table belongs to
    schema_name (string): The schema the table belong to
    table_name (string): The table schema and table name
    help_flag (bool): If True, prints the docstring of this function

    Examples:
    get_delta_stats(table_location = "schema_name.table_name")

    """

    if help_flag:
        print(inspect.getdoc(get_delta_stats))
        return
    
    if (table_name == None and schema_name != None):
        return

    table_location = f"{catalog_name}.{schema_name}.{table_name}"

    # Gather metrics
    file_details = spark.sql(f"DESCRIBE DETAIL {table_location}")
    file_features = file_details.select(explode("tableFeatures"))
    file_count = file_details.select("numFiles").first()[0]
    file_location = file_details.select("location").first()[0]
    file_created = file_details.select("createdAt").first()[0]
    file_modified = file_details.select("lastModified").first()[0]
    total_size = file_details.select("sizeInBytes").first()[0]

    file_sizes = dbutils.fs.ls(file_location)

    # Extracting the desired information
    data_to_convert = [
        {
            "path": file.path,
            "name": file.name, 
            "size": file.size
        } 
        for file in file_sizes
    ]

    # Convert the data to JSON
    file_json_data = convert_to_json(data_to_convert)

    # Assuming spark session is already created and table_location is defined
    table_history = spark.sql(f"DESCRIBE HISTORY {table_location} LIMIT 5").collect()

    # Preparing the data for conversion
    data_to_convert = [
        {
            "Version": record.version,
            "Timestamp": record.timestamp.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),  # Formatting timestamp
            "Operation": record.operation,
        }
        for record in table_history
    ]

    # Convert the data to JSON
    history_json_data = convert_to_json(data_to_convert)

    partition_column = get_partition_column(table_location)
    partition_count = None
    data_skewness = []
    partition_json_data = []

    if partition_column:
        # Get the count of distinct partitions
        partition_count = spark.sql(
            f"SELECT COUNT(DISTINCT {partition_column}) AS partition_count FROM {table_location}"
        ).first()[0]

        # Get the top 5 partitions by record count
        data_skewness = spark.sql(
            f"SELECT {partition_column}, COUNT(*) as record_count FROM {table_location} GROUP BY {partition_column} ORDER BY record_count DESC LIMIT 5"
        ).collect()

        # Extracting the desired information
        data_to_convert = [
            {
                "partition_name": record[0].strftime('%Y-%m-%d') if isinstance(record[0], date) else record[0],
                "partition_size": record[1]
            } 
            for record in data_skewness
        ]

        # Convert the data to JSON
        partition_json_data = convert_to_json(data_to_convert)

    # Running the DESCRIBE EXTENDED query
    describe_result = spark.sql("DESCRIBE EXTENDED citi_bike.raw_bike_data").collect()

    # Parsing the result to find catalog name, schema name, and table name
    catalog_name = None
    schema_name = None
    table_name = None

    for row in describe_result:
        if row['col_name'] == 'Catalog':
            catalog_name = row['data_type']
        elif row['col_name'] == 'Database':
            schema_name = row['data_type']
        elif row['col_name'] == 'Table':
            table_name = row['data_type']

    # Prepare data for DataFrame
    results_data = [
        Row(metric="Catalog Name", value=str(catalog_name)),
        Row(metric="Schema Name", value=str(schema_name)),
        Row(metric="Table Name", value=str(table_name)),
        Row(metric="File Count", value=str(file_count)),
        Row(metric="File Location", value=str(file_location)),
        Row(metric="File Created", value=str(file_created)),
        Row(metric="File Modified", value=str(file_modified)),
        Row(metric="Total Size (bytes)", value=str(total_size)),
        Row(metric="File_sizes", value=str(file_json_data)),
        Row(metric="Partition Column", value=str(partition_column)),
        Row(metric="Partition Count", value=str(partition_count)),
        Row(metric="Partition Sizes (Top 5)", value=str(partition_json_data)),
        Row(metric="Table_History (Last 5)", value=str(history_json_data)),
    ]

    # Create DataFrame
    results_df = spark.createDataFrame(results_data)

    # Pivoting
    pivoted_df = results_df.groupBy().pivot("metric").agg(first("value"))

    display_df = pivoted_df.select("Catalog Name", "Schema Name", "Table Name",
                                   "File Count", "File Location", "File Created",
                                   "File Modified", "Total Size (bytes)", "File_sizes",
                                   "Partition Column", "Partition Count", "Partition Sizes (Top 5)", "Table_History (Last 5)"
                                   )
    # Show results
    display_df.display()

# COMMAND ----------

# Run the main function with user input
# table_location = input("Enter Delta Table Location: ")
help_flag = False
get_delta_stats("hive_metastore","citi_bike","dim_bikes", help_flag)

# COMMAND ----------

DATABRICKS_HOST = ""
TOKEN = ""

# COMMAND ----------

def get_query_history(cluster_id):
    # Endpoint to get query history
    endpoint = f"{DATABRICKS_HOST}/api/2.0/sql/history/queries"

    # Headers for authentication
    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }

    # Parameters for the request
    params = {
        "cluster_id": cluster_id,
    }

    # Send a GET request
    response = requests.get(endpoint, headers=headers, params=params)

    # Check for successful response
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

# COMMAND ----------

cluster_id = "2e4d88ed9a34a54f"
try:
    query_history = get_query_history(cluster_id)
    print(json.dumps(query_history, indent=4))
except Exception as e:
    print(e)
