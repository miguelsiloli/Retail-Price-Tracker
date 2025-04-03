

## Refined TDD Test Suites (Prioritized)

---

### 1. Data Model (`StagingProduct` - `tests/test_models.py`)

**All Essential:** These tests ensure the fundamental integrity and structure of the data we aim to produce.

*   **`test_create_valid_instance` (Essential):**
    *   *Explanation:* Can we successfully create a data record when we have all the right information in the correct format?
*   **`test_missing_required_field_error` (Essential):**
    *   *Explanation:* Does the system correctly report an error if mandatory information (like Product ID or Name) is missing?
*   **`test_invalid_data_type_error` (Essential):**
    *   *Explanation:* Does the system catch errors if we provide text where a number is expected, or an invalid web address for the product URL?
*   **`test_invalid_currency_format_error` (Essential):**
    *   *Explanation:* Does the system ensure the currency code follows the standard 3-letter format (like "EUR", "USD")?
*   **`test_extra_field_error` (Essential):**
    *   *Explanation:* Does the system prevent adding unexpected extra information that doesn't belong in the standard record?

---

### 2. Configuration Loader (`config_loader.py` - `tests/test_config_loader.py`)

**All Essential:** These ensure the system can start correctly by reading its instructions.

*   **`test_load_valid_yaml_config` (Essential):**
    *   *Explanation:* Can the system successfully read a correctly formatted configuration file?
*   **`test_load_nonexistent_file_error` (Essential):**
    *   *Explanation:* Does the system handle the situation properly if the specified configuration file doesn't exist?
*   **`test_load_invalid_yaml_error` (Essential):**
    *   *Explanation:* Does the system handle errors if the configuration file has incorrect syntax (like missing colons or bad indentation)?
*   **`test_load_config_missing_required_key_error` (Essential):**
    *   *Explanation:* Does the system check if critical configuration sections (like the source name or parser settings) are present in the file?

---

### 3. Task Generators (`components/generator.py` - `tests/components/test_generator.py`)

*   **Essential Tests (for `StaticUrl` & `CategoryList`):**
    *   **`test_yields_correct_initial_tasks` (Essential):**
        *   *Explanation:* Does the generator correctly create the starting web addresses (tasks) based on the list of URLs or categories provided in the config?
    *   **`test_applies_static_params` (Essential):**
        *   *Explanation:* Does the generator correctly add any standard parameters (like store ID or language) required for every web address?
    *   **`test_yields_metadata` (Essential):**
        *   *Explanation:* Does each generated task include helpful context, like which starting URL or category it came from?
    *   **`test_handles_empty_input_list` (Essential):**
        *   *Explanation:* Does the generator behave correctly (do nothing) if the configuration provides an empty list of starting URLs or categories?
*   **Optional / Feature-Specific Tests:**
    *   **`test_reads_input_from_csv` (Optional):**
        *   *Explanation:* If we configure it to read starting URLs or categories from a CSV file, does it do so correctly? (Only needed if this feature is implemented).
    *   **`test_handles_param_increment_pagination` (Optional/Deferred):**
        *   *Explanation:* Can the generator itself handle simple page-number-based pagination if configured? (This adds complexity and might be handled later or elsewhere).
*   **Deferred Implementation Tests:**
    *   **`test_sitemap_generator_is_placeholder` (Essential - for now):**
        *   *Explanation:* Does the sitemap generator (which isn't fully built yet) correctly do nothing for now?

---

### 4. Fetcher (`components/fetcher.py` - `tests/components/test_fetcher.py`)

**All Essential:** These ensure the system can reliably get web page content.

*   **`test_fetch_successful_request` (Essential):**
    *   *Explanation:* Can the fetcher successfully download the content of a web page when everything works correctly?
*   **`test_fetch_sends_correct_headers_and_params` (Essential):**
    *   *Explanation:* Does the fetcher send the correct identifying information (headers) and any required parameters when requesting a page?
*   **`test_fetch_retries_on_temporary_error` (Essential):**
    *   *Explanation:* If the website is temporarily unavailable (like a server error 503 or a timeout), does the fetcher automatically try again a few times? (Test *one* common retry scenario like 503 or timeout).
*   **`test_fetch_raises_exception_after_exhausting_retries` (Essential):**
    *   *Explanation:* If the website remains unavailable after several retries, does the fetcher correctly report a failure?
*   **`test_fetch_handles_permanent_error_without_retry` (Essential):**
    *   *Explanation:* If the fetcher encounters an error that won't fix itself (like "Page Not Found" - 404), does it fail immediately without retrying unnecessarily?

---

### 5. Parser (`components/parser.py` - `tests/components/test_parser.py`)

**All Essential:** These ensure we can accurately pull information out of the downloaded web pages.

*   **`test_parse_extracts_multiple_products` (Essential):**
    *   *Explanation:* Can the parser find all the individual product sections on a page listing multiple products?
*   **`test_parse_extracts_correct_data` (Essential):** Combine `text_and_attributes` test.
    *   *Explanation:* Does the parser correctly extract the specific pieces of information (like name, price, image link) we defined for each product?
*   **`test_parse_handles_list_extraction` (Essential):** Combine `list_true` test.
    *   *Explanation:* If a product has multiple features listed, can the parser extract all of them as a list?
*   **`test_parse_extracts_embedded_json` (Essential - Simplified V1):**
    *   *Explanation:* Can the parser find and extract structured data (JSON) sometimes hidden within the page code?
*   **`test_parse_handles_malformed_embedded_json` (Essential):**
    *   *Explanation:* If the hidden structured data (JSON) is broken, does the parser handle it without crashing and report null for that field?
*   **`test_parse_skips_item_if_mandatory_field_missing` (Essential):**
    *   *Explanation:* If a product is missing crucial information (marked as mandatory), does the parser correctly skip that product record?
*   **`test_parse_handles_non_mandatory_missing_field` (Essential):**
    *   *Explanation:* If a product is missing optional information, does the parser still process the product but leave that specific field blank (null)?
*   **`test_parse_returns_empty_list_if_no_products_found` (Essential):**
    *   *Explanation:* If a page is loaded but contains no products matching our definition, does the parser correctly return an empty list?
*   **`test_parse_finds_next_page_link` (Essential):** Combine next page tests.
    *   *Explanation:* Can the parser find the link to the "Next Page" if one exists, and correctly determine its full web address (even if relative)?
*   **`test_parse_handles_no_next_page_link` (Essential):** Combine missing/mismatch tests.
    *   *Explanation:* If there is no "Next Page" link, or if pagination isn't configured for this page type, does the parser correctly report that there's no next page?

---

### 6. Schema Mapper (`components/mapper.py` - `tests/components/test_mapper.py`)

**All Essential:** These ensure the extracted raw data is correctly transformed into our standard final format.

*   **`test_map_direct_field_mapping` (Essential):**
    *   *Explanation:* Can the mapper correctly copy data from a specific extracted field (e.g., 'pid') to the corresponding standard field (e.g., 'product_id')?
*   **`test_map_literal_value_mapping` (Essential):** Combine string/number tests.
    *   *Explanation:* Can the mapper correctly assign fixed, hardcoded values (like setting `currency` to always be "EUR") defined in the config?
*   **`test_map_list_creation` (Essential):** Combine list creation/none tests.
    *   *Explanation:* Can the mapper correctly create a list containing a single item (like an image URL) based on the config rule? Does it create an empty list if the item is missing?
*   **`test_map_nested_field_access` (Essential):** Combine dot notation/missing path tests.
    *   *Explanation:* Can the mapper access data nested within the extracted information (e.g., getting 'sku' from inside 'details')? Does it handle missing nested data gracefully?
*   **`test_map_attributes_raw_assembly` (Essential):**
    *   *Explanation:* Can the mapper group various extra extracted fields into the special 'attributes_raw' dictionary as defined in the config?
*   **`test_map_adds_standard_metadata` (Essential):**
    *   *Explanation:* Does the mapper ensure that standard run information (like the source name and scrape time) is added to every record?
*   **`test_map_handles_missing_intermediate_field` (Essential):**
    *   *Explanation:* If the parser failed to extract a field that the mapper expects, does the mapper handle this without crashing (setting the final field to null)?
*   **`test_map_validation_on_output` (Essential):** Combine Pydantic error test.
    *   *Explanation:* Before finishing, does the mapper use the final data model (`StagingProduct`) to validate the record, catching any remaining type or format errors and preventing bad data from proceeding?

---

### 7. Data Persistence (`components/persistence.py` - `tests/components/test_persistence.py`)

*   **Essential Tests:**
    *   **`test_save_processed_data_correct_location_and_name` (Essential):**
        *   *Explanation:* Does the system save the main product data file to the correct folder (based on source name) and with the correct filename (including source name and date)?
    *   **`test_save_processed_data_valid_format` (Essential):**
        *   *Explanation:* Is the saved product data file actually readable and in the correct Parquet format?
    *   **`test_save_metrics_correct_location_and_name` (Essential):**
        *   *Explanation:* Does the system save the run statistics (metrics) file to the correct folder (based on source name) and with the correct filename?
    *   **`test_save_metrics_valid_format` (Essential):**
        *   *Explanation:* Is the saved metrics file readable and in the correct JSON format?
*   **Optional Tests:**
    *   **`test_save_handles_filesystem_errors` (Optional):**
        *   *Explanation:* Does the system react appropriately (e.g., report an error) if it doesn't have permission to write files in the target directory? (Harder to test reliably).

---

### 8. Rate Limiter (`components/limiter.py` - `tests/components/test_limiter.py`)

*   **Essential Tests:**
    *   **`test_first_call_is_immediate` (Essential):**
        *   *Explanation:* Does the rate limiter allow the very first request for a source to happen instantly?
    *   **`test_subsequent_calls_are_delayed` (Essential):** Combine subsequent/no_wait tests.
        *   *Explanation:* Does the rate limiter enforce the configured waiting period between consecutive requests? Does it correctly calculate *not* waiting if enough time has already passed?
*   **Optional Tests:**
    *   **`test_limiter_is_thread_safe` (Optional/Difficult):**
        *   *Explanation:* Does the rate limiter correctly manage delays even when multiple parts of the system are trying to make requests simultaneously? (Primarily verified by careful code review).

---

### 9. Scraper Framework (`framework.py` - `tests/test_framework.py`)

**All Essential (as integration tests):** These ensure the whole system works together.

*   **`test_run_source_simple_success` (Essential):**
    *   *Explanation:* Can the framework run a simple scrape from start to finish (generate task, fetch page, parse items, map data, save results/metrics) without errors?
*   **`test_run_source_handles_pagination` (Essential):**
    *   *Explanation:* Can the framework correctly handle multi-page listings by getting the "Next Page" link from the parser and automatically fetching/processing subsequent pages?
*   **`test_run_source_aggregates_metrics` (Essential):**
    *   *Explanation:* Does the framework correctly combine the statistics (metrics) collected from processing each individual page into a final summary for the entire run?
*   **`test_run_source_resilient_to_task_error` (Essential):** Combine fetch/parse/map error tests.
    *   *Explanation:* If fetching, parsing, or mapping fails for *one* specific page or item, does the framework log the error and continue processing other pages/items without crashing the whole run? (Test *one* representative error scenario).

---