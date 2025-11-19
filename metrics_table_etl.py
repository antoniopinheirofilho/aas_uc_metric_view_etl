# Databricks notebook source
# MAGIC %md
# MAGIC # Metrics Table ETL Job
# MAGIC
# MAGIC This job processes data partitions based on configuration from YAML file.
# MAGIC Supports both daily and monthly partitions, as well as non-partitioned target tables.
# MAGIC
# MAGIC See the Read Me notebook for additional information on ETL YAML spec.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC - **`config_file`** _(required)_
# MAGIC
# MAGIC     Path in the workspace to the YAML **configuration file** with the definitions for the target table to be loaded.
# MAGIC
# MAGIC     _Can be either relative or absolute path_ 
# MAGIC
# MAGIC - **`dry_run`** _(optional)_
# MAGIC
# MAGIC     Set to 'true' to validate without writing 
# MAGIC     
# MAGIC     _Defaults to 'false'_
# MAGIC
# MAGIC - **`target_date`** _(optional)_
# MAGIC
# MAGIC     The **date** or **date range** to process. Should match target table's **partition column format**
# MAGIC
# MAGIC     _Defaults configured offset of current date._
# MAGIC
# MAGIC     _Accepted formats:_
# MAGIC     - single day: `YYYY-MM-DD`
# MAGIC     - date range: `YYYY-MM-DD,YYYY-MM-DD`
# MAGIC     - single month: `YYYY-MM`
# MAGIC     - month range: `YYYY-MM,YYYY-MM`

# COMMAND ----------

dbutils.widgets.text("config_file", "config_file_goes_here.yaml", "Configuration File Path")
dbutils.widgets.text("target_date", "", "Target Date")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry Run")

config_file = dbutils.widgets.get("config_file")
target_date = dbutils.widgets.get("target_date")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"

print(f"config_file: {config_file}")
print(f"target_date: {target_date}")
print(f"dry_run: {dry_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper functions definition

# COMMAND ----------

# MAGIC %pip install -qqqq pyyaml

# COMMAND ----------

import re
from datetime import datetime, timedelta
from pyspark.sql.functions import expr, count, min, max
from builtins import min as math_min, max as math_max
import json
import yaml
import os

# COMMAND ----------

# Utility Functions #

def str_to_datetime(date_str, fmt, regex, partition_type):
    """
    Validates that the input string matches the given regex and format.
    Returns a datetime object if valid, raises ValueError otherwise.

    Args:
        date_str (str): The date or month string to validate.
        fmt (str): The datetime format string (e.g., "%Y-%m-%d" or "%Y-%m").
        regex (str): The regex pattern the string must match.
        label (str): Label for error messages ("Date" or "Month").

    Returns:
        datetime: Parsed datetime object.

    Raises:
        ValueError: If the input is not valid for the given format or regex.
    """
    if not re.match(regex, date_str):
        raise ValueError(f"{partition_type.capitalize()} '{date_str}' is not in {fmt.replace('%Y', 'YYYY').replace('%m', 'MM').replace('%d', 'DD')} format")
    try:
        return datetime.strptime(date_str, fmt)
    except Exception:
        raise ValueError(f"{partition_type.capitalize()} '{date_str}' is not a valid {partition_type.lower()} in {fmt.replace('%Y', 'YYYY').replace('%m', 'MM').replace('%d', 'DD')} format")

def convert_to_date_obj(date_str, partition_type):
    """
    Validates that the input string is a valid date or month depending on partition_type.
    Returns a datetime object if valid, raises ValueError otherwise.

    Args:
        date_str (str): The date or month string to validate.
        partition_type (str): "day" for YYYY-MM-DD, "month" for YYYY-MM.

    Returns:
        datetime: Parsed datetime object.

    Raises:
        ValueError: If the input is not valid for the given partition_type.
    """
    if not isinstance(date_str, str):
        raise ValueError(f"{partition_type.capitalize()} must be a string in {'YYYY-MM-DD' if partition_type == 'day' else 'YYYY-MM'} format")

    if partition_type == "day":
        return str_to_datetime(date_str, "%Y-%m-%d", r"^\d{4}-\d{2}-\d{2}$", partition_type)
    elif partition_type == "month":
        return str_to_datetime(date_str, "%Y-%m", r"^\d{4}-\d{2}$", partition_type)
    else:
        raise ValueError(f"Unknown partition_type '{partition_type}' for date validation")

def parse_range(start_part, end_part, partition_type):
    """
    Parse a date or month range and return (start, end) as strings.

    Args:
        start_part (str): Start date/month string.
        end_part (str): End date/month string.
        partition_type (str): "day" for YYYY-MM-DD, "month" for YYYY-MM.

    Returns:
        tuple: (start, end) as strings in the appropriate format.
    """
    try:
        start_date = convert_to_date_obj(start_part, partition_type)
        end_date = convert_to_date_obj(end_part, partition_type)
    except Exception as e:
        raise ValueError(f"Invalid date range for {partition_type} partition: '{start_part},{end_part}'. {str(e)}")

    if start_date > end_date:
        raise ValueError(f"Invalid date range for {partition_type} partition: '{start_part}' is after '{end_part}'")
    
    if partition_type == "day":
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    else:
        start_date = start_date.replace(day=1)
        # Get last day of month in end_date
        if end_date.month == 12:
            end_date = end_date.replace(year=end_date.year + 1, month=1)
        else:
            end_date = end_date.replace(month=end_date.month + 1)
        end_date -= timedelta(days=1)
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def parse_target_date(partition_type, date_input):
    """
    Parse date input to handle single dates, single months, or date ranges.

    Args:
        partition_type (str): Partition type (day or month)
        date_input (str): Date input, should match partition_type. 

    Returns:
        tuple: (start_date, end_date) in the appropriate format
    """

    if partition_type not in ["day", "month"]:
        raise ValueError(f"target_date is not supported for partition type: {partition_type}, only 'day' or 'month' partition types")

    if not date_input or not isinstance(date_input, str):
        raise ValueError("date_input must be a non-empty string")

    if ',' not in date_input:
        # Single date or month, use it as both start and end
        return parse_range(date_input, date_input, partition_type)
    else:
        # Range (contains comma), split into start and end
        parts = date_input.split(',')
        if len(parts) != 2:
            raise ValueError(f"Invalid date range format: {date_input}. Expected format: YYYY-MM-DD,YYYY-MM-DD or YYYY-MM,YYYY-MM")
        start_part = parts[0].strip()
        end_part = parts[1].strip()
        return parse_range(start_part, end_part, partition_type)
    
def log(level, msg):
    print(f"[{level}] {msg}")

def validate_configuration(config):
    """
    Comprehensive configuration validation with detailed error messages
    
    Args:
        config (dict): Configuration dictionary to validate
        
    Returns:
        dict: Validated configuration with proper types
        
    Raises:
        ValueError: If configuration is invalid with detailed error messages
    """
    if not config or not isinstance(config, dict):
        raise ValueError("Configuration must be a non-empty dictionary")
    
    # Required fields validation
    required_fields = ['table', 'schema', 'partition_type', 'partition_query', 'processing_strategy']
    missing_fields = [field for field in required_fields if field not in config or config[field] is None]
    
    if missing_fields:
        raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")
    
    # Extract and validate basic fields
    table = config.get('table', '').strip()
    schema = config.get('schema', '').strip()
    partition_type = config.get('partition_type', '').strip()
    partition_query = config.get('partition_query', '').strip()
    partition_column = config.get('partition_column', '').strip() if config.get('partition_column') else ''
    processing_strategy = config.get('processing_strategy', '').strip()
    
    # Validate table
    if not table:
        raise ValueError("table must be a non-empty string")
    
    # Validate schema
    if not schema:
        raise ValueError("schema must be a non-empty string")
    
    # Validate partition_type
    valid_partition_types = ['day', 'month', 'full']
    if partition_type not in valid_partition_types:
        raise ValueError(f"partition_type must be one of: {', '.join(valid_partition_types)}. Got: '{partition_type}'")
    
    # Validate partition_query
    if not partition_query:
        raise ValueError("partition_query must be a non-empty string")
        
    # Validate view-specific configuration
    if processing_strategy == 'view':
        if partition_type != 'full':
            raise ValueError("Views can only use 'full' partition type")
        if partition_column:
            raise ValueError("Views do not support partition_column parameter")
        if target_date:
            raise ValueError("Views do not support target_date parameter")
    
    ##### partition_query should be single SELECT statement, also accepting WITH clause
    
    # Validate based on partition type
    if partition_type == "full":
        # Full partition validation                
        if partition_column:
            raise ValueError("Full partition type does not support partition_column parameter")
        
        if "{target_date_list}" in partition_query:
            raise ValueError("Full partition type does not support '{target_date_list}' placeholder in partition_query")
        
        # Validate processing_strategy for full partition
        if not processing_strategy:
            raise ValueError("processing_strategy must be defined for full partition type")
        
        valid_full_strategies = ['full', 'view']
        if processing_strategy not in valid_full_strategies:
            raise ValueError(f"processing_strategy for full partition type must be one of: {', '.join(valid_full_strategies)}. Got: '{processing_strategy}'")
    
    elif partition_type in ["day", "month"]:
        # Partitioned load validation
        if not partition_column:
            raise ValueError(f"partition_column must be defined for {partition_type} partition type")
        
        # Validate processing_strategy
        if not processing_strategy:
            raise ValueError(f"processing_strategy must be defined for {partition_type} partition type")
        
        if partition_type == "day":
            valid_day_strategies = ['single_day', 'last_and_current_month']
            if processing_strategy not in valid_day_strategies:
                raise ValueError(f"processing_strategy for day partition type must be one of: {', '.join(valid_day_strategies)}. Got: '{processing_strategy}'")
        elif partition_type == "month":
            valid_month_strategies = ['single_month']
            if processing_strategy not in valid_month_strategies:
                raise ValueError(f"processing_strategy for month partition type must be one of: {', '.join(valid_month_strategies)}. Got: '{processing_strategy}'")
        
        # Validate partition_query contains required placeholder
        if "{target_date_list}" not in partition_query:
            raise ValueError(f"partition_query must contain '{{target_date_list}}' placeholder for {partition_type} partition type")
        
    
    # Validate target table format (basic check)
    if table.count('.') > 0:
        raise ValueError(f"table should be only the table note, without schema and catalog, got: '{table}'")
    
    # Validate target table format (basic check)
    if schema.count('.') > 0:
        raise ValueError(f"schema should be only the schema name, without the catalog, got: '{schema}'")
    
    # Return validated config
    return config

def load_config_from_yaml(config_file_path):
    """
    Load configuration from YAML file
    
    Args:
        config_file_path (str): Path to the YAML configuration file.
        
    Returns:
        dict: Validated configuration dictionary
    """
    
    if not config_file_path or not isinstance(config_file_path, str) or config_file_path.strip() == "":
        raise ValueError("Configuration file path must be defined and non-empty")
    
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Configuration file not found: {config_file_path}")
    
    try:
        with open(config_file_path, 'r') as file:
            config = yaml.safe_load(file)
        
        if config is None:
            raise ValueError("Configuration file is empty or contains no valid YAML")
        
        # Validate the configuration
        return validate_configuration(config)
        
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML format in configuration file: {str(e)}")
    except Exception as e:
        raise ValueError(f"Error loading configuration file: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare
# MAGIC
# MAGIC Setup the environment for the partition processing job.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Configuration

# COMMAND ----------

# Load configuration from YAML file
config = load_config_from_yaml(config_file)

# Set defaults
catalog = "your_catalog"

# Extract validated configuration values
schema = config['schema']
table = config['table']
partition_column = config.get('partition_column', '')
partition_type = config['partition_type']
processing_strategy = config['processing_strategy']
partition_query = config['partition_query']

log("info", f"Configuration loaded:")
log("info", f"  Destination table: {table}")
log("info", f"  Destination schema: {schema}")
log("info", f"  Destination catalog: {catalog}")
log("info", f"  Partition column: {partition_column}")
log("info", f"  Partition type: {partition_type}")
log("info", f"  Processing strategy: {processing_strategy}")

target_table = f"{catalog}.{schema}.{table}"
log("info", f"  Full table name: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Target Dates
# MAGIC Calculate the target dates based on partition type, offset and reprocess length

# COMMAND ----------

import pytz
import re
def to_year_month(date_str):
    return re.sub(r'^(\d{4}-\d{2}).*', '\\1', date_str)

def calculate_date_range_from_strategy(partition_type, processing_strategy, timezone):
    """
    Calculate start and end dates based on processing strategy.
    
    Args:
        partition_type (str): "day", "month", or "full"
        processing_strategy (str): The processing strategy to use
        timezone: The timezone to use for date calculations
        
    Returns:
        tuple: (start_date, end_date) as strings in appropriate format
    """
    current_date = datetime.now(tz=timezone)
    previous_day = current_date - timedelta(days=1)
    
    if partition_type == "day":
        if processing_strategy == "single_day":
            # Process the previous day (D-1)
            end_date_dt = previous_day
            start_date_dt = previous_day
            return start_date_dt.strftime("%Y-%m-%d"), end_date_dt.strftime("%Y-%m-%d")
            
        elif processing_strategy == "last_and_current_month":
            # Process every day in the last month and current month up to D-1
            # Start from the first day of the previous month
            last_day_prev_month = previous_day.replace(day=1) - timedelta(days=1)
            first_day_prev_month = last_day_prev_month.replace(day=1)
            
            # End at the previous day (D-1)
            end_date_dt = previous_day
            
            return first_day_prev_month.strftime("%Y-%m-%d"), end_date_dt.strftime("%Y-%m-%d")
            
        else:
            raise ValueError(f"Unsupported processing strategy for day partition: {processing_strategy}")
            
    elif partition_type == "month":
        if processing_strategy == "single_month":
            # Process the month of the previous day up to D-1
            start_date_dt = previous_day.replace(day=1)
            
            # End at the previous day (D-1)
            end_date_dt = previous_day
            
            return start_date_dt.strftime("%Y-%m-%d"), end_date_dt.strftime("%Y-%m-%d")
            
        else:
            raise ValueError(f"Unsupported processing strategy for month partition: {processing_strategy}")
            
    elif partition_type == "full":
        if processing_strategy == "full":
            # For full partition type, no date range is calculated
            return None, None
        else:
            raise ValueError(f"Unsupported processing strategy for full partition: {processing_strategy}")
            
    else:
        raise ValueError(f"Unsupported partition type: {partition_type}")


# Parse target_date input
timezone=pytz.timezone("UTC")  # Change to your timezone as needed
start_date = None
end_date = None
start_date_formatted = None
send_date_formatted = None

if partition_type in ["day", "month"]:
    if target_date:
        log("info", f"Using provided target date: {target_date}")
        start_date, end_date = parse_target_date(partition_type, target_date)
        if start_date is None or end_date is None:
            raise ValueError("Failed to parse target_date")
    else:
        log("info", f"No target date provided, using defined processing strategy: {processing_strategy}")
        
        start_date, end_date = calculate_date_range_from_strategy(partition_type, processing_strategy, timezone)

    if partition_type == "month":
        start_date_formatted = to_year_month(start_date)
        end_date_formatted = to_year_month(end_date)
    else:
        start_date_formatted = start_date
        end_date_formatted = end_date
    
    log("info", f"Date range: {start_date} to {end_date}")
    log("info", f"Date range formatted: {start_date_formatted} to {end_date_formatted}")
elif partition_type == "full":
    # For full partition type, no date range is calculated
    start_date = None
    end_date = None
    start_date_formatted = None
    end_date_formatted = None
    if target_date:
        raise ValueError("Partition type 'full' does not support target_date parameter")
    log("info", "Full partition type: no date range calculation needed")
else:
    raise ValueError(f"Unsupported partition type: {partition_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Partition Query

# COMMAND ----------

# Generate the date list for the IN clause
if partition_type == "full":
    date_list = []
elif start_date == end_date:
    date_list = [start_date]
else:
    date_list = []
    month_set = set()
    start_dt = convert_to_date_obj(start_date, "day")
    end_dt = convert_to_date_obj(end_date, "day")
    current_dt = start_dt
    while current_dt <= end_dt:
        date_list.append(current_dt.strftime('%Y-%m-%d'))
        if partition_type == "month":
            month_set.add(current_dt.strftime('%Y-%m'))
        current_dt += timedelta(days=1)
    month_list = list(sorted(month_set))

# For SQL query, build the string for the IN clause
if partition_type == "full":
    query_date_list_sql = None
    target_date_list_sql = None
    target_date_list = None
elif partition_type == "day":
    query_date_list_sql = ", ".join([f"'{d}'" for d in date_list])
    target_date_list_sql = query_date_list_sql
    target_date_list = date_list
elif partition_type == "month":
    query_date_list_sql = ", ".join([f"'{d}'" for d in date_list])
    target_date_list_sql = ", ".join([f"'{d}'" for d in month_list])
    target_date_list = month_list

# Replace the placeholder in the query
if query_date_list_sql:
    partition_query = partition_query.replace("{target_date_list}", query_date_list_sql)

log("info", f"Partition query:\n\n{partition_query}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Partition Overwrite Condition
# MAGIC This condition is used to choose the partitions from target table to be written/overwritten.

# COMMAND ----------

# Build partition condition using start_date and end_date (inclusive)
if partition_type == "full":
    partition_overwrite_condition = None
    log("info", "No partition condition, partition type is 'full'")
else:
    partition_overwrite_condition = f"{partition_column} IN ({target_date_list_sql})"
    log("info", f"Target table partition overwrite condition: {partition_overwrite_condition}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Partition(s)
# MAGIC
# MAGIC Read, validate and write the data to the target table's partition(s)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Source Data

# COMMAND ----------

log("info", "Reading source data...")
start_time = datetime.now()

try:
    partition_df = spark.sql(partition_query)
except Exception as e:
    log("error", f"Error reading source data with partition_query: {partition_query}")
    raise ValueError(f"Failed to execute partition_query due to syntax or execution error: {str(e)}")

log("info", "Source data read.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Data Quality Checks

# COMMAND ----------

# Data quality checks
if processing_strategy == "view":
    # For views, we just validate that the query returns data
    try:
        row_count = partition_df.count()
        log("info", f"View query test returned {row_count} rows")
        
        if row_count == 0:
            log("warning", "View query returns no rows - this might be expected for some views")
            
    except Exception as e:
        error_msg = f"Error validating view query: {str(e)}"
        log("error", error_msg)
        raise ValueError(error_msg)
        
elif partition_type == "full":
    row_count = partition_df.count()
    log("info", f"Data read row count: {row_count}")

    if row_count == 0:
        error_msg = f"No data found with 'partition_query' ('partition_type={partition_type}')"
        log("error", error_msg)
        raise ValueError(error_msg)

elif partition_type in ["day", "month"]:
    row_count_by_partition_df = (
        partition_df
        .groupBy(partition_column)
        .count()
        .orderBy(partition_column)
    )

    log("info", f"Row count for each partition read:")
    row_count_by_partition_df.display()

    partitions_read = [str(row[partition_column]) for row in row_count_by_partition_df.collect()]

    if not partitions_read:
        error_msg = f"No data found with 'partition_query' ('partition_type={partition_type}') for date range {start_date_formatted} to {end_date_formatted}"
        log("error", error_msg)
        raise ValueError(error_msg)
    else:
        min_date = math_min(partitions_read)
        max_date = math_max(partitions_read)

        log("info", f"Date range ({partition_column}) on read data: {min_date} to {max_date}")

        if (min_date < start_date_formatted or max_date > end_date_formatted):
            msg = f"{partition_column} values outside expected range {start_date_formatted} to {end_date_formatted}: min={min_date}, max={max_date}"
            log("error", msg)
            raise ValueError(msg)

    # Check every date in target_date_list has rows in partition_df
    missing_dates = []
    for date in target_date_list:
        if date not in partitions_read:
            missing_dates.append(date)

    if missing_dates:
        msg = f"Data missing for the following {partition_type} partition(s) in '{partition_column}': {missing_dates}. Expected date range from {start_date_formatted} to {end_date_formatted}."
        log("warning", msg)

        # For each missing date, check if it exists in the target table (already processed)
        target_table_dates_in_range_with_data = []
        if spark.catalog.tableExists(target_table):
            target_table_dates_in_range_with_data_df = (
                spark.table(target_table)
                .select(partition_column)
                .where(expr(partition_overwrite_condition))
                .distinct()
            )
            target_table_dates_in_range_with_data = [str(row[partition_column]) for row in target_table_dates_in_range_with_data_df.collect()]

        missing_dates_that_has_data_in_target_table = []
        for date in missing_dates:
            if date in target_table_dates_in_range_with_data:
                missing_dates_that_has_data_in_target_table.append(date)

        if missing_dates_that_has_data_in_target_table:
            msg = f"Data missing for partition(s) on target table, would result in data gap: {missing_dates_that_has_data_in_target_table}."
            log("error", msg)
            raise ValueError(msg)
        else:
            log("info", f"Data missing only for empty partition(s) on target table, proceding.")

    # Validate target table schema
    log("info", "Validating target table partition column is present in the query result...")
    if partition_column not in partition_df.columns:
        raise ValueError(f"Target partition column '{partition_column}' not found in query result columns: {partition_df.columns}")

else:
    raise ValueError(f"Invalid partition type '{partition_type}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Target Table

# COMMAND ----------

log("info", f"Target table: {target_table}");

# COMMAND ----------

log("info", f"Writing to target table: {target_table}")

if not dry_run:

    try:
        is_new_table = not spark.catalog.tableExists(target_table)
        if is_new_table:
            log("info", f"Target table '{target_table}' does not exist and will be created.")

        if processing_strategy == "view":
            # For views, we create or replace the view using the partition_query
            view_sql = f"CREATE OR REPLACE VIEW {target_table} AS {partition_query}"
            log("info", f"Creating view with query: {view_sql}")
            spark.sql(view_sql)
            log("info", "View creation/update completed successfully!")
        else:
            writer = partition_df.write.format("delta").option("mergeSchema", "true") # new columns will be automatically added to the table schema

            if partition_type in ["day", "month"]:
                writer.mode("overwrite").option("replaceWhere", partition_overwrite_condition).saveAsTable(target_table)
                log("info", "Partition processing completed successfully!")
            elif partition_type == "full":
                writer.mode("overwrite").saveAsTable(target_table)
                log("info", "Full table processing completed successfully!")
            else:
                raise ValueError(f"Unsupported partition type: '{partition_type}'")

            if is_new_table and partition_type != "full":
                log("info", f"Enabling liquid clustering on '{target_table}' for column '{partition_column}'...")
                try:
                    spark.sql(f"ALTER TABLE {target_table} CLUSTER BY ({partition_column})").display()
                    log("info", f"Enabled liquid clustering.")
                except Exception as e:
                    log("warning", f"Could not set liquid clustering on '{target_table}': {str(e)}")

    except Exception as e:
        log("error", f"Error writing to target table: {str(e)}")
        raise
else:
    log("info", "DRY RUN: Skipping write operation")
    log("info", f"Would write {row_count} rows to table {target_table} for partition {start_date_formatted} to {end_date_formatted}")

end_time = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize

# COMMAND ----------

if processing_strategy == "view":
    log("info", "Skipping optimize operation for view")

elif not dry_run:
    log("info", f"Optimizing '{target_table}'...")
    
    try:
        spark.sql(f"OPTIMIZE {target_table}").display()
    except Exception as e:
        log("warning", f"OPTIMIZE failed on '{target_table}': {str(e)}")

else:
    log("info", "DRY RUN: Skipping optimize operation")

# COMMAND ----------

if processing_strategy == "view":
    log("info", "Skipping vacuum operation for view")

elif not dry_run:
    log("info", f"Vacuuming '{target_table}'...")
    try:
        spark.sql(f"VACUUM {target_table}").display()
    except Exception as e:
        log("warning", f"VACUUM failed on '{target_table}': {str(e)}")
else:
    log("info", "DRY RUN: Skipping vacuum operation")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate

# COMMAND ----------

if not dry_run:

    log("info", f"Verification - Target table: {target_table}")

    if partition_type == "full":
        row_count = spark.table(target_table).count()

        log("info", f"Verification - Rows in target table: {row_count}")

    elif partition_type in ["day", "month"]:
        row_count_by_partition_on_target_df = (
            partition_df
            .groupBy(partition_column)
            .count()
            .orderBy(partition_column)
        )

        log("info", f"Row count for each processed partition on target table:")
        row_count_by_partition_on_target_df.display()

        row_count_by_partition_on_target = {str(row[partition_column]): row['count'] for row in row_count_by_partition_on_target_df.collect()}

        row_count = sum(row_count_by_partition_on_target.values())
        min_date = math_min(row_count_by_partition_on_target.keys()) if row_count_by_partition_on_target else None
        max_date = math_max(row_count_by_partition_on_target.keys()) if row_count_by_partition_on_target else None

        log("info", f"Verification - Total rows in target table processed partition(s): {row_count}")
        log("info", f"Verification - Date range: {min_date} to {max_date}")

    else:
        raise ValueError(f"Unsupported partition type: '{partition_type}'")

    log("info", f"Partition processing job completed successfully.") 

else:
    log("info", "DRY RUN: Skipping post-write verification, displaying partition_df instead")
    
    partition_df.display()