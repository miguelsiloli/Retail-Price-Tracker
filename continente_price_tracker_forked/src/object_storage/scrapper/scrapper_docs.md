```json
[
  {
    "metric_name": "total_num_api_calls",
    "data_type": "Integer",
    "description": "Total number of successful HTTP GET requests made to the Auchan API endpoint (Search-UpdateGrid) to fetch product pages.",
    "nullable": false
  },
  {
    "metric_name": "num_rows",
    "data_type": "Integer",
    "description": "Total number of product rows included in the final aggregated Parquet file. Will be 0 if no products were found or processed.",
    "nullable": false
  },
  {
    "metric_name": "data_size_raw_bytes",
    "data_type": "Integer",
    "description": "Estimated total memory usage in bytes of the final pandas DataFrame in memory *before* saving to Parquet, calculated using `df.memory_usage(deep=True).sum()`. Will be 0 if the DataFrame is empty.",
    "nullable": false
  },
  {
    "metric_name": "data_size_compressed_bytes",
    "data_type": "Integer",
    "description": "Actual size in bytes of the generated Parquet file on disk after compression. Can be null if the file saving failed or was skipped.",
    "nullable": true
  },
  {
    "metric_name": "estimated_compression_ratio",
    "data_type": "Float",
    "description": "The ratio of compressed file size to estimated raw data size (data_size_compressed_bytes / data_size_raw_bytes). Indicates the effectiveness of Parquet compression. Can be null if compressed size is null or raw size is 0.",
    "nullable": true
  },
  {
    "metric_name": "total_elapsed_time_seconds",
    "data_type": "Float",
    "description": "Total wall-clock time in seconds taken for the `run_auchan_pipeline` function to execute from start to finish.",
    "nullable": false
  }
```