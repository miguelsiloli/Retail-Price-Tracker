# Configuration Schema Documentation

**Version:** 1.0
**Date:** YYYY-MM-DD

## Purpose

This document defines the structure, keys, data types, and purpose of the YAML configuration files used for each data source in the scraping system. This schema focuses on providing the necessary parameters for reliable data fetching, task generation, basic parsing, and schema mapping for the **Processed Staging Layer**. Complex transformations, post-processing, and detailed validation are intended for downstream processes.

## Root Level Keys

| Key           | Type            | Mandatory | Default | Description                                                                 |
| :------------ | :-------------- | :-------- | :------ | :-------------------------------------------------------------------------- |
| `source_name` | `string`        | Yes       | -       | Unique identifier for the data source (lowercase, no spaces, e.g., "auchan"). |
| `enabled`     | `boolean`       | Yes       | `true`  | Whether the scraper for this source should be run by the orchestrator.      |
| `fetcher`     | `object (dict)` | Yes       | -       | Configuration related to fetching data (see details below).                 |
| `task_generation` | `object (dict)` | Yes       | -       | Configuration for how to identify the initial URLs/tasks to scrape.        |
| `pagination`  | `object (dict)` | Yes       | -       | Configuration for handling multi-page results.                            |
| `parser`      | `object (dict)` | Yes       | -       | Configuration for extracting data from fetched content.                      |
| `schema_mapping` | `object (dict)` | Yes       | -       | Rules for mapping extracted fields to the canonical staging schema fields.  |
| `concurrency` | `object (dict)` | No        | `{ type: threading, num_workers: 1 }` | Settings for parallel execution within the source run.                 |
| `output_paths`| `object (dict)` | Yes       | -       | Base directory paths for pipeline outputs.                                  |

## `fetcher` Section

| Key                | Type                     | Mandatory | Default | Description                                                                                                      |
| :----------------- | :----------------------- | :-------- | :------ | :--------------------------------------------------------------------------------------------------------------- |
| `fetcher_type`     | `string`                 | Yes       | `"requests_html"` | The method to use for fetching. Currently only `"requests_html"` is supported.                             |
| `base_url`         | `string`                 | No        | `null`  | Base URL for the source, used if task generation or pagination uses relative paths. Often optional.            |
| `request_headers`  | `object (dict[str, str])`| No        | `{}`    | Key-value pairs for HTTP headers (e.g., `User-Agent`, `Accept`).                                                 |
| `retry_config`     | `object (dict)`          | No        | `{ retries: 3, delay: 60 }` | Settings for retrying failed requests.                                                           |
| `politeness_delay` | `list[float or int, float or int]` | No        | `[2, 5]` | Range `[min, max]` in seconds for a random delay between requests. Min and Max must be non-negative. |

### `fetcher.retry_config` Sub-Section

| Key       | Type           | Mandatory | Default | Description                             |
| :-------- | :------------- | :-------- | :------ | :-------------------------------------- |
| `retries` | `integer`      | No        | `3`     | Maximum number of retry attempts.       |
| `delay`   | `float` or `int` | No        | `60`    | Delay in seconds between retry attempts. |

## `task_generation` Section

Must contain exactly one of the following types:

| Key                 | Type                | Mandatory | Description                                                                                             |
| :------------------ | :------------------ | :-------- | :------------------------------------------------------------------------------------------------------ |
| `type`              | `string`            | Yes       | Specifies the task generation method. Must be one of: `"static_url_list"`, `"category_list"`, `"sitemap_xml"`. |
| `urls`              | `list[string]`      | Yes (if `type` is `"static_url_list"`) | A direct list of absolute URLs to scrape.                                                 |
| `category_param`    | `string`            | Yes (if `type` is `"category_list"`) | The URL query parameter name used for the category identifier (e.g., "cgid", "categoria"). |
| `category_values`   | `list[string]`      | Yes (if `type` is `"category_list"`) | The list of category identifier values to be used with `category_param`.                    |
| `category_base_url` | `string`            | Yes (if `type` is `"category_list"`) | The base URL to which `?category_param=category_value` will be appended.                 |
| `sitemap_url`       | `string`            | Yes (if `type` is `"sitemap_xml"`) | The URL of the sitemap index or sitemap file to parse.                                   |
| `sitemap_filter_pattern` | `string` (regex) | No        | Optional regex pattern to filter URLs found in the sitemap (e.g., only include product URLs). |
| `static_params`     | `object (dict[str, str])` | No    | Optional static query parameters to add to *every* generated task URL.                       |

## `pagination` Section

Must contain exactly one of the following types (or `type: none`):

| Key                   | Type             | Mandatory | Default | Description                                                                                                                                 |
| :-------------------- | :--------------- | :-------- | :------ | :------------------------------------------------------------------------------------------------------------------------------------------ |
| `type`                | `string`         | Yes       | `"none"`| Specifies the pagination method. Must be one of: `"none"`, `"param_increment"`, `"next_page_selector"`, `"load_more_button"`.                 |
| `param_name`          | `string`         | Yes (if `type` is `"param_increment"`) | The name of the URL query parameter representing the page or start index (e.g., "page", "start", "cp").                     |
| `page_size`           | `integer`        | Yes (if `type` is `"param_increment"`) | The number of items per page (used to calculate the 'start' offset if `param_name` refers to start index, or just for checks). |
| `start_index_base`    | `integer (0 or 1)` | No (if `type` is `"param_increment"`) | `1`     | Whether the `param_name` starts at 0 or 1 for the first page.                                                             |
| `next_page_selector`  | `string`         | Yes (if `type` is `"next_page_selector"`) | CSS selector for the 'Next' page link (`<a>` tag). The framework will follow the `href` attribute.                          |
| `last_page_indicator` | `string`         | No (if `type` is `"next_page_selector"`) | `null`  | Optional CSS selector for an element indicating the *absence* of a next page (e.g., a disabled 'Next' button). If not specified, pagination stops when `next_page_selector` is not found. |
| `load_more_selector`  | `string`         | Yes (if `type` is `"load_more_button"`) | CSS selector for the 'Load More' button. *Requires Selenium/JS execution, currently not supported by requests_html fetcher.* |
| `load_more_max_clicks`| `integer`        | No (if `type` is `"load_more_button"`) | `50`    | Safety limit for the number of times the 'Load More' button will be clicked.                                              |
| `total_count_selector`| `string`         | No        | `null`  | Optional CSS selector for an element containing the total number of items. If found, can be used to optimize `param_increment` pagination.    |

## `parser` Section

| Key                        | Type                  | Mandatory | Default | Description                                                                                                                                                                                           |
| :------------------------- | :-------------------- | :-------- | :------ | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `product_container_selector` | `string`              | Yes       | -       | CSS selector identifying the repeating HTML element that contains the data for a single product.                                                                                                      |
| `fields`                   | `object (dict[str, object])` | Yes       | -       | Defines the intermediate fields to extract from each product container. The keys are the intermediate field names (e.g., `pid`, `name`, `price_raw`). See `parser.fields` sub-section below. |

### `parser.fields` Sub-Section (Definition for each field)

Each key under `fields` is the intermediate name for the extracted data. The value is an object with the following keys:

| Key                  | Type                    | Mandatory | Default            | Description                                                                                                                                                           |
| :------------------- | :---------------------- | :-------- | :----------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `selector`           | `string`                | Yes       | -                  | CSS selector relative to `product_container_selector` to find the element containing the data.                                                                        |
| `attribute`          | `string`                | No        | `null` (use text)  | Optional: The HTML attribute to extract the value from (e.g., "href", "src", "data-pid"). If null/omitted, the element's text content is used.                          |
| `type`               | `string`                | Yes       | `"str"`            | Target data type (`str`, `float`, `int`, `bool`). Basic conversion is attempted. If conversion fails, the value typically becomes null (or raises error based on `mandatory`). |
| `mandatory`          | `boolean`               | No        | `false`            | If `true`, the field must be found and have a non-null value after type conversion. If validation fails, the product record might be skipped or flagged.                 |
| `list`               | `boolean`               | No        | `false`            | If `true`, the `selector` is expected to match multiple elements. The specified `attribute` (or text) will be extracted from *each* matched element, forming a list.     |
| `_source_selector`   | `string`                | No        | `null`             | Advanced: CSS selector (relative to container) for an *outer* element holding nested data (e.g., in an attribute). Use with `_source_attribute` and `_parse_as`.        |
| `_source_attribute`  | `string`                | No        | `null`             | Advanced: Attribute of the `_source_selector` element containing the nested data (e.g., "data-json").                                                                  |
| `_parse_as`          | `string ("json")`       | No        | `null`             | Advanced: If set to `"json"`, treats the value from `_source_attribute`/`_source_selector` text as a JSON string and parses it before extracting sub-fields/paths.      |
| `path`               | `string` or `list[str]` | No        | `null`             | Advanced: Used with `_parse_as: json`. Specifies the key or path (using dot notation for nested keys, e.g., "details.color") within the parsed JSON to extract the value. |
| `_subfields`         | `object (dict)`         | No        | `null`             | Advanced: Used with `_parse_as: json`. Defines a nested dictionary structure. Keys are sub-field names, values are objects defining `path`, `type`, `mandatory`.         |

**Note:** Validation rules and post-processing functions are explicitly excluded at this stage as per requirements. Data types are relaxed; focus is on reliable extraction into the specified basic types (`str`, `int`, `float`, `bool`, `list`).

## `schema_mapping` Section

| Key                      | Type                  | Mandatory | Default | Description                                                                                                                               |
| :----------------------- | :-------------------- | :-------- | :------ | :---------------------------------------------------------------------------------------------------------------------------------------- |
| `(canonical_field_name)` | `string`              | No        | -       | Key: Name of the field in the **Canonical Staging Schema**. Value: Name of the **intermediate field** (from `parser.fields`) to map from. |
| `...`                    | `string`              | ...       | ...     | Include one entry for each canonical field you want to populate from the intermediate fields extracted by the parser.                     |

**Example:** `product_id: pid` maps the intermediate field `pid` to the canonical `product_id`. Dot notation (`category_l1: category_info.level_1`) is supported for accessing nested intermediate fields.

## `concurrency` Section

| Key           | Type      | Mandatory | Default     | Description                                                  |
| :------------ | :-------- | :-------- | :---------- | :----------------------------------------------------------- |
| `type`        | `string`  | No        | `"threading"` | Concurrency model. Currently only `"threading"` is supported. |
| `num_workers` | `integer` | No        | `1`         | Number of worker threads to use for fetching/parsing tasks.  |

## `output_paths` Section

| Key         | Type     | Mandatory | Default | Description                                                              |
| :---------- | :------- | :-------- | :------ | :----------------------------------------------------------------------- |
| `processed` | `string` | Yes       | -       | Base directory path for processed data output (e.g., Parquet files).   |
| `metrics`   | `string` | Yes       | -       | Base directory path for metrics output (JSON files).                     |
| `logs`      | `string` | Yes       | -       | Base directory path for log file output.                                 |
| `monitoring`| `string` | No        | `null`  | Optional: Base directory path for instance/deployment monitoring files. |

---