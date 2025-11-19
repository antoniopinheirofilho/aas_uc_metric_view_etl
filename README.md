# How to Create a New YAML Config

This guide explains how to create a new YAML configuration file for the ETL process.

## Basic Structure

Every YAML config file should contain these required fields:

```yaml
  
  table: "destination_table_name"
  schema: "destination_schema"
  partition_column: ""
  partition_type: "full"
  processing_strategy: "full"
  partition_query: |
      SELECT *
      FROM your_source_table
  
```

## Configuration Fields

### Required Fields

- **`table`**: The destination table name (without schema)

- **`schema`**: The destination schema with catalog

- **`partition_column`**: The column used for partitioning (empty string for no partitioning)

- **`partition_type`**: Type of partitioning
  - `"full"` - No partitioning, process all data
  - `"day"` - Daily partitions (YYYY-MM-DD format)
  - `"month"` - Monthly partitions (YYYY-MM format)

- **`processing_strategy`**: Defines which partitions or data range to process, depending on the `partition_type`. Available strategies:
  - For `partition_type` = `"full"`:
    - `"full"`: Process all data (no partitioning)
  - For `partition_type` = `"day"`:
    - `"single_day"`: Process only the previous day
    - `"last_and_current_month"`: Process all days in the last month and the current month up to yesterday
  - For `partition_type` = `"month"`:
    - `"single_month"`: Process yesterday's month
  - For `partition_type` = `"full"`:
    - `"view"`: No preprocessing is done

- **`partition_query`**: The SQL query to extract data from source

### Query Guidelines

1. **For non-partitioned tables** (`partition_type: "full"`), no condition needed:
    ```yaml
      
      partition_query: |
          SELECT *
          FROM your_source_table
      
    ```

2. **For partitioned tables** (`partition_type: "day"` or `"month"`), you **should** include the partition condition for the `partition_column`:

    ```sql

      IN ({target_date_list})
      
    ```

   Example:

    ```yaml

      partition_query: |     
          SELECT *
          FROM your_source_table
          WHERE partition_column IN ({target_date_list})
      
    ```

   Other conditions can be added with `AND`. Example:

    ```yaml
     
      partition_query: |     
          SELECT *
          FROM your_source_table
          WHERE partition_column IN ({target_date_list})
          AND (other_column >= 0 OR another_column IS NULL)
     
    ```

3. **Column transformations**: You can rename columns, cast types, or add computed columns:
    ```yaml

      partition_query: |
          SELECT 
              source.column1 AS new_name,
              CAST(source.column2 AS STRING) AS string_column,
              CASE 
                  WHEN source.condition THEN 'value1'
                  ELSE 'value2'
              END AS computed_column
          FROM your_source_table source
       
    ```

## Examples

### Simple Dimension Table
```yaml
  
  table: "dim_product"
  schema: "your_schema"
  partition_column: ""
  partition_type: "full"
  processing_strategy: "full"
  partition_query: |
      SELECT 
          product_id as product_code,
          product_name,
          product_category,
          created_date
      FROM your_catalog.source_schema.source_product_table
  
```

### Partitioned Fact Table
```yaml

  table: "fact_sales"
  schema: "your_schema"
  partition_column: "sale_date"
  partition_type: "day"
  processing_strategy: "single_day"
  partition_query: |
      SELECT 
          source.sale_date,
          source.location_id,
          source.quantity_sold,
          source.sale_amount
      FROM your_catalog.source_schema.source_sales_table source
      WHERE source.sale_date IN ({target_date_list})
  
```

## Naming Convention

- Use descriptive table names (e.g., `dim_product`, `fact_sales`)
- Prefer snake_case for table and column names

## Tips

1. Test your query separately before adding it to the YAML
2. For joins always use table aliases
4. Make sure your source table exists and is accessible
5. Verify that the destination schema exists