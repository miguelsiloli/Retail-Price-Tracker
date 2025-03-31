# config_template.yaml
# Version: 1.0
# Template for configuring a data source for the scraping system.

# --- Basic Info ---
source_name: "example_source" # REQUIRED: Unique identifier (lowercase, no spaces)
enabled: true                 # REQUIRED: Set to false to disable runs for this source

# --- Fetcher Configuration ---
fetcher:
  fetcher_type: "requests_html" # REQUIRED: Currently only "requests_html"
  # base_url: "https://www.example.com" # Optional: Base URL if needed for relative paths
  request_headers: # Optional: Custom HTTP headers
    User-Agent: "MyScraperBot/1.0 (+http://mywebsite.com/botinfo)"
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    Accept-Language: "en-US,en;q=0.5"
  retry_config: # Optional: Defaults to { retries: 3, delay: 60 }
    retries: 3
    delay: 60 # Seconds
  politeness_delay: [2, 5] # Optional: Defaults to [2, 5]. Random delay range [min, max] in seconds between requests.

# --- Task Generation ---
# Choose ONE method: static_url_list, category_list, or sitemap_xml
task_generation:
  type: "category_list" # REQUIRED: "static_url_list", "category_list", or "sitemap_xml"

  # --- Options for type: "static_url_list" ---
  # urls: # REQUIRED if type is static_url_list
  #   - "https://www.example.com/products/page1"
  #   - "https://www.example.com/products/page2"

  # --- Options for type: "category_list" ---
  category_param: "cat_id" # REQUIRED if type is category_list: Query param name
  category_values: # REQUIRED if type is category_list: List of category IDs/slugs
    - "electronics"
    - "clothing"
    - "home-goods"
  category_base_url: "https://www.example.com/browse" # REQUIRED if type is category_list: Base URL for categories

  # --- Options for type: "sitemap_xml" ---
  # sitemap_url: "https://www.example.com/sitemap.xml" # REQUIRED if type is sitemap_xml
  # sitemap_filter_pattern: "/product/" # Optional: Regex to filter URLs from sitemap

  # --- Optional Static Parameters for ALL task types ---
  # static_params: # Optional: Add these query params to every generated URL
  #   store_id: "123"
  #   lang: "en"

# --- Pagination ---
# Choose ONE method or "none"
pagination:
  type: "param_increment" # REQUIRED: "none", "param_increment", "next_page_selector" # "load_more_button" needs JS
  # Optional: Selector for total item count, helps optimize param_increment
  # total_count_selector: "span.total-results-count"

  # --- Options for type: "param_increment" ---
  param_name: "page" # REQUIRED if type is param_increment
  page_size: 24 # REQUIRED if type is param_increment
  start_index_base: 1 # Optional: Defaults to 1 (page=1 is first). Set to 0 if param starts at 0.

  # --- Options for type: "next_page_selector" ---
  # next_page_selector: "a.pagination__next" # REQUIRED if type is next_page_selector
  # last_page_indicator: "a.pagination__next[disabled]" # Optional: Selector for disabled next link

  # --- Options for type: "load_more_button" (Requires JS - Currently Not Supported by requests_html fetcher) ---
  # load_more_selector: "button.load-more-results" # REQUIRED if type is load_more_button
  # load_more_max_clicks: 50 # Optional: Safety limit, defaults to 50

# --- Parser Configuration ---
parser:
  # REQUIRED: CSS selector for the element containing a single product
  product_container_selector: "div.product-item"

  # REQUIRED: Defines how to extract intermediate fields from each container
  fields:
    # --- Example: Simple text extraction ---
    intermediate_name:
      selector: "h2.product-title a"
      # attribute: null (default) -> extract text
      type: "str" # Default type
      mandatory: true # This field must be found
    # --- Example: Attribute extraction ---
    intermediate_id:
      selector: "div.product-data"
      attribute: "data-product-sku"
      type: "str"
      mandatory: true
    # --- Example: Price extraction (float) ---
    intermediate_price:
      selector: "span.price-final"
      # attribute: null (default) -> extract text, needs cleaning downstream or simple type conversion
      type: "float"
      mandatory: false # Allow items without price
    # --- Example: Image URL ---
    intermediate_image:
      selector: "img.product-image"
      attribute: "src"
      type: "str"
      mandatory: false
    # --- Example: Extracting multiple items into a list ---
    intermediate_features:
      selector: "ul.feature-list li"
      # attribute: null (default) -> extract text
      type: "str" # Type of items *within* the list
      list: true # Indicates selector matches multiple elements
      mandatory: false
    # --- Example: Extracting from JSON embedded in attribute ---
    intermediate_json_data:
        _source_selector: "script[type='application/ld+json']" # Find the script tag
        # attribute: null -> use text content of the script tag
        _parse_as: "json" # Parse the text content as JSON
        # Now define sub-fields using path (or _subfields for nested dict output)
        # Example flattening:
        # path: "name" # Extract value of "name" key from the parsed JSON
        # type: "str"
        # mandatory: false
        # Example nested output:
        _subfields:
          json_sku: { path: "sku", type: "str", mandatory: false }
          json_brand: { path: "brand.name", type: "str", mandatory: false }

# --- Schema Mapping ---
# REQUIRED: Maps intermediate field names (keys in parser.fields) to
#           Canonical Staging Schema field names (values).
# Only fields listed here as KEYS will be included in the final staging output.
schema_mapping:
  product_id: intermediate_id        # Map intermediate 'intermediate_id' to canonical 'product_id'
  source_product_id: intermediate_id # Also map it here for reference
  product_name: intermediate_name    # Map intermediate 'intermediate_name'
  price_current: intermediate_price  # Map intermediate 'intermediate_price'
  image_urls: '[intermediate_image]' # Special syntax example: create list from single item
  # brand: intermediate_brand_field # Map brand if extracted
  # category_raw: intermediate_category_field # Map raw category string/slug
  # Add mappings for ALL canonical fields you want to populate for this source

# --- Concurrency ---
concurrency: # Optional: Defaults to { type: threading, num_workers: 1 }
  type: "threading" # Currently only "threading"
  num_workers: 1 # Number of threads for fetching/parsing tasks

# --- Output Paths ---
output_paths: # REQUIRED: Base directories for output files
  processed: "data/processed/example_source" # For Parquet data files
  metrics: "data/metrics/example_source"     # For JSON metrics files
  logs: "logs/example_source"                # For .log files (filename includes source+timestamp)
  # monitoring: "data/monitoring/example_source" # Optional: For instance monitoring data