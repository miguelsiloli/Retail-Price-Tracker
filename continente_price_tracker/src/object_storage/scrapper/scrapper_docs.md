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

```
./data/
├── processed/        # Stores cleaned, structured data (latest run per day)
│   ├── {source}/
│   │   └── auchan_products_{datetime.format(YYYY_MM_DD)}.parquet
│   ├── continente/
│   │   └── continente_products_YYYY_MM_DD.parquet
│   ├── pingodoce/
│   │   └── pingodoce_products_YYYY_MM_DD.parquet
│   ├── minipreco/
│   │   └── minipreco_products_YYYY_MM_DD.parquet
│   ├── mercadonapt/
│   │   └── mercadonapt_products_YYYY_MM_DD.parquet
│   ├── intermarchept/
│   │   └── intermarchept_products_YYYY_MM_DD.parquet
│   ├── lidlpt/
│   │   └── lidlpt_products_YYYY_MM_DD.parquet
│   ├── elcorteinglespt/
│   │   └── elcorteinglespt_products_YYYY_MM_DD.parquet
│   ├── apolonia/
│   │   └── apolonia_products_YYYY_MM_DD.parquet
│   ├── sparpt/
│   │   └── sparpt_products_YYYY_MM_DD.parquet
│   └── coviranpt/
│       └── coviranpt_products_YYYY_MM_DD.parquet
│
├── metrics/          # Stores metrics about the latest scrape run per day
│   ├── {source}/
│   │   └── auchan_metrics_{datetime.format(YYYY_MM_DD)}.json
│   ├── continente/
│   │   └── continente_metrics_YYYY_MM_DD.json
│   ├── pingodoce/
│   │   └── pingodoce_metrics_YYYY_MM_DD.json
│   ├── minipreco/
│   │   └── minipreco_metrics_YYYY_MM_DD.json
│   ├── mercadonapt/
│   │   └── mercadonapt_metrics_YYYY_MM_DD.json
│   ├── intermarchept/
│   │   └── intermarchept_metrics_YYYY_MM_DD.json
│   ├── lidlpt/
│   │   └── lidlpt_metrics_YYYY_MM_DD.json
│   ├── elcorteinglespt/
│   │   └── elcorteinglespt_metrics_YYYY_MM_DD.json
│   ├── apolonia/
│   │   └── apolonia_metrics_YYYY_MM_DD.json
│   ├── sparpt/
│   │   └── sparpt_metrics_YYYY_MM_DD.json
│   └── coviranpt/
│       └── coviranpt_metrics_YYYY_MM_DD.json
│
└── logs/             # Stores log files for each scraper run (granular timestamp recommended)
    ├── {source}/
    │   └── auchan_pipeline_YYYY_MM_DD.log  # Keeping granular timestamp
    ├── continente/
    │   └── continente_pipeline_YYYY_MM_DD.log
    ├── pingodoce/
    │   └── pingodoce_pipeline_YYYY_MM_DD.log
    ├── minipreco/
    │   └── minipreco_pipeline_YYYY_MM_DD.log
    ├── mercadonapt/
    │   └── mercadonapt_pipeline_YYYY_MM_DD.log
    ├── intermarchept/
    │   └── intermarchept_pipeline_YYYY_MM_DD.log
    ├── lidlpt/
    │   └── lidlpt_pipeline_YYYY_MM_DD.log
    ├── elcorteinglespt/
    │   └── elcorteinglespt_pipeline_YYYY_MM_DD.log
    ├── apolonia/
    │   └── apolonia_pipeline_YYYY_MM_DD.log
    ├── sparpt/
    │   └── sparpt_pipeline_YYYY_MM_DD.log
    └── coviranpt/
        └── coviranpt_pipeline_YYYY_MM_DD.log
```

Metrics value:

- Entity-Attribute-Value (EAV) Model Representation: This is a more specific data modeling term that fits your example very well.

    Entity: The scraper run itself (which isn't explicitly in each object but is the context for the whole array).

    Attribute: The metric_name (e.g., "num_rows", "total_elapsed_time_seconds").

    Value: The metric_value (e.g., 12345, 345.67).
    Your JSON is essentially a list of attribute-value pairs describing the scraper run entity.

```json
[
  {
    "metric_name": "data_size_compressed_bytes",
    "metric_value": null
  },
  {
    "metric_name": "data_size_raw_bytes",
    "metric_value": 0
  },
  {
    "metric_name": "estimated_compression_ratio",
    "metric_value": null
  },
  {
    "metric_name": "num_rows",
    "metric_value": 0
  },
  {
    "metric_name": "total_elapsed_time_seconds",
    "metric_value": 25.12
  },
  {
    "metric_name": "total_num_api_calls",
    "metric_value": 5
  }
]
```