# Key Component Interface Definitions

**Version:** 1.0
**Date:** YYYY-MM-DD

## Purpose

This document defines the method signatures, parameters, return types, and error handling contracts for the primary interactions between the core components of the scalable scraper system. Adhering to these interfaces ensures modularity and smooth integration.

---

## 1. `ScraperFramework` -> `TaskGenerator`

### `generateTasks(task_config: Dict) -> List[Dict]`

*   **Purpose:** Generates the initial list of atomic scraping tasks (e.g., URLs with specific parameters) based on the source's configuration.
*   **Parameters:**
    *   `task_config` (`Dict`): The `task_generation` section loaded from the source's YAML configuration file. Structure depends on the specified `type` (e.g., `static_url_list`, `category_list`, `sitemap_xml`).
*   **Returns:**
    *   `List[Dict]`: A list where each dictionary represents a single task to be processed by a worker.
    *   **Minimum Required Keys per Task Dictionary:**
        *   `'url'` (`str`): The absolute URL to be fetched for this task.
    *   **Potential Additional Keys per Task Dictionary:**
        *   Source-specific identifiers (e.g., `'category_slug': str`) if relevant to the task type and needed downstream.
        *   Static parameters combined into the URL or passed separately if the fetcher needs them distinctly.
*   **Error Handling:**
    *   If task generation fails due to invalid configuration, inaccessible resources (e.g., bad sitemap URL, missing CSV file), or other fatal errors:
        *   Logs a `CRITICAL` error message detailing the failure.
        *   Returns an empty list (`[]`) to signal failure to the `ScraperFramework` without halting the entire application.

---

## 2. `ScraperFramework` (Worker) -> `Fetcher`

### `fetch(url: str, params: Optional[Dict], headers: Dict, retry_config: Dict, limiter: RateLimiter, worker_metrics: Dict) -> str`

*   **Purpose:** Fetches the raw content (HTML/JSON) for a single URL, handling retries and rate limiting.
*   **Parameters:**
    *   `url` (`str`): The absolute URL to fetch.
    *   `params` (`Optional[Dict]`): Dictionary of URL query parameters (or `None`).
    *   `headers` (`Dict`): Dictionary of HTTP request headers.
    *   `retry_config` (`Dict`): Dictionary containing retry parameters (e.g., `{'retries': int, 'delay': float}`).
    *   `limiter` (`RateLimiter`): The shared RateLimiter instance for this source run, used to enforce politeness delays.
    *   `worker_metrics` (`Dict`): The mutable dictionary holding metrics *for the current worker thread*. The fetcher increments counters like `'api_calls'` in this dictionary upon successful fetches.
*   **Returns:**
    *   `str`: The raw response body content (e.g., HTML source, JSON string) on successful fetch after potential retries.
*   **Exceptions (on Unrecoverable Failure):**
    *   If all retries are exhausted due to network issues, timeouts, or non-successful HTTP status codes (e.g., 4xx, 5xx), the method should raise an exception that indicates failure. Documented primary exceptions:
        *   `requests.exceptions.Timeout`: If a request times out after retries.
        *   `requests.exceptions.RequestException`: Base class for most other request-related errors (connection errors, invalid URL, bad status codes after retries). A custom `MaxRetriesExceededError` wrapping these could also be used.
*   **Side Effects:**
    *   Calls `limiter.waitForNextRequest()` before making the request.
    *   Increments relevant counters (e.g., `api_calls`) in the passed `worker_metrics` dictionary upon success.
    *   Logs retry attempts and final failures.

---

## 3. `ScraperFramework` (Worker) -> `Parser`

### `parse(raw_content: str, parser_config: Dict) -> List[Dict]`

*   **Purpose:** Parses the raw fetched content to extract structured data for products/items found on the page, based on source-specific configuration.
*   **Parameters:**
    *   `raw_content` (`str`): The raw HTML source or JSON string returned by the `Fetcher`.
    *   `parser_config` (`Dict`): The `parser` section loaded from the source's YAML configuration file, containing `product_container_selector` and the detailed `fields` definitions.
*   **Returns:**
    *   `List[Dict]`: A list of dictionaries. Each dictionary represents one successfully parsed product/item.
    *   The keys within each dictionary correspond to the **intermediate field names** defined in `parser_config.fields`.
    *   Returns an empty list (`[]`) if `product_container_selector` finds no matching elements or if a fundamental parsing error occurs (e.g., invalid HTML/JSON structure preventing processing).
*   **Error Handling:**
    *   If `product_container_selector` finds no elements: Logs an `INFO` or `DEBUG` message and returns `[]`.
    *   If parsing a *single item* fails within a page (e.g., mandatory field missing, type conversion fails for a mandatory field): Logs a `WARNING` including context (e.g., the problematic intermediate field name and potentially part of the source element). Skips that item and continues parsing other items on the page.
    *   If a fundamental error occurs preventing page parsing (e.g., `BeautifulSoup` or `json.loads` fails on `raw_content`): Logs an `ERROR` and returns `[]` for that page/task.

---

## 4. `ScraperFramework` (Worker) -> `SchemaMapper`

### `mapToSchema(parsed_item: Dict, schema_mapping_config: Dict, standard_metadata: Dict) -> Dict`

*   **Purpose:** Transforms a single dictionary of parsed data (with intermediate field names) into a dictionary conforming to the **Canonical Staging Schema**. Adds standard metadata.
*   **Parameters:**
    *   `parsed_item` (`Dict`): A single dictionary representing one product/item, as produced by the `Parser` (keys are intermediate field names).
    *   `schema_mapping_config` (`Dict`): The `schema_mapping` section loaded from the source's YAML, defining the mapping from intermediate keys to canonical keys.
    *   `standard_metadata` (`Dict`): A dictionary containing standard metadata values to be added, typically `{'source': str, 'scraped_timestamp': str (ISO 8601 UTC)}`.
*   **Returns:**
    *   `Dict`: A dictionary representing the product/item conforming to the Canonical Staging Schema. Keys are the canonical field names. Includes the added `standard_metadata`.
*   **Error Handling:**
    *   If an `intermediate_field_name` specified as a *value* in `schema_mapping_config` is *not found* as a key in the input `parsed_item`:
        *   Logs a `WARNING` message indicating the missing intermediate field and the canonical field it was supposed to map to.
        *   The corresponding canonical field in the output dictionary will have a value of `None`.
    *   Handles dot notation specified in `schema_mapping_config` values to access nested data within `parsed_item`. Logs a warning and results in `None` if the path is invalid.
    *   Type conversions defined in the canonical schema (like ensuring timestamp format) are applied here. Failures might log warnings or errors depending on severity.

---

## 5. `ScraperFramework` -> `DataPersistence`

### `saveProcessedData(data: pd.DataFrame, output_base_path: str, source_name: str, timestamp: datetime) -> str`

*   **Purpose:** Saves the final aggregated and processed data DataFrame to a file (e.g., Parquet).
*   **Parameters:**
    *   `data` (`pd.DataFrame`): The Pandas DataFrame containing the aggregated data for the source run, conforming to the Canonical Staging Schema.
    *   `output_base_path` (`str`): The base directory path for processed data (e.g., value from `output_paths.processed`).
    *   `source_name` (`str`): The unique identifier of the source.
    *   `timestamp` (`datetime`): The start timestamp of the scrape run, used for filename generation (e.g., formatting to `YYYY_MM_DD`).
*   **Returns:**
    *   `str`: The full, absolute path to the saved data file upon successful completion.
*   **Exceptions:**
    *   Raises standard Python file I/O exceptions on failure, such as:
        *   `IOError` / `OSError`: General file system errors.
        *   `PermissionError`: Insufficient permissions to write to the target directory/file.
        *   Potentially errors from the Parquet writer library (`pyarrow.lib.ArrowIOError`, etc.).

### `saveMetrics(metrics: List[Dict], output_base_path: str, source_name: str, timestamp: datetime) -> str`

*   **Purpose:** Saves the collected metrics for the run to a JSON file.
*   **Parameters:**
    *   `metrics` (`List[Dict]`): The final aggregated list of metric dictionaries (each dict: `{'metric_name': str, 'metric_value': Any}`).
    *   `output_base_path` (`str`): The base directory path for metrics data (e.g., value from `output_paths.metrics`).
    *   `source_name` (`str`): The unique identifier of the source.
    *   `timestamp` (`datetime`): The start timestamp of the scrape run, used for filename generation.
*   **Returns:**
    *   `str`: The full, absolute path to the saved metrics file upon successful completion.
*   **Exceptions:**
    *   Raises standard Python file I/O exceptions on failure:
        *   `IOError` / `OSError`
        *   `PermissionError`
        *   `TypeError`: If the metrics data is not JSON serializable.

---

## 6. Interaction with `MetricsCollector`

*(Note: Based on refinement, direct interaction might be limited if using per-thread dictionaries. This describes the interface if a central collector *were* used, or the methods needed for the final aggregation.)*

### `incrementCounter(metric_name: str, value: int = 1)`

*   **Purpose:** Atomically increments a named counter metric.
*   **Thread Safety:** **MUST** be implemented using appropriate locking (`threading.Lock`) if used concurrently by multiple threads on a shared instance. *Recommended alternative: Use per-thread dicts.*

### `recordTiming(metric_name: str, start_time: float, end_time: float)` *(Example if used)*

*   **Purpose:** Records the duration for a specific operation.
*   **Thread Safety:** Requires locking if updating shared state.

### `getMetrics() -> List[Dict]`

*   **Purpose:** Retrieves the final collected metrics in the standard list-of-dictionaries format.
*   **Thread Safety:** Accessing the final state should ideally happen after all threads are joined, or requires locking if accessed while threads might still be writing.

---

## 7. Interaction with `RateLimiter`

### `waitForNextRequest()`

*   **Purpose:** Blocks the calling thread until the configured politeness delay has elapsed since the last recorded request *across all threads*.
*   **Parameters:** None.
*   **Returns:** `None`.
*   **Thread Safety:** **MUST** be implemented using appropriate locking (`threading.Lock`) to ensure only one thread updates the `lastRequestTime` and calculates the wait duration at a time, coordinating the delay across all threads using the limiter.

---