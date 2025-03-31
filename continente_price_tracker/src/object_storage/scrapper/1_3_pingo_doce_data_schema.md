# --- config/pingodoce.yaml ---
# ... other config sections (fetcher, parser, etc.) ...

# parser_config:
#   product_container_selector: 'div.product-cards' # Example
#   fields:
#     pid_extracted: { selector: 'a.product-cards__link', attribute: 'href', post_processing: [{ function: extract_id_from_url_path }] } # Example extraction logic
#     name: { selector: 'h3.product-cards__title' }
#     price_val: { selector: 'span.product-cards_price', type: float, post_processing: [{ function: clean_price_string }] }
#     rating_info: { selector: 'div.bv_text', mandatory: false }
#     url_main: { selector: 'a.product-cards__link', attribute: 'href' }
#     category_slug: { # This might be passed down from the task generator rather than parsed from the page }

schema_mapping: # Maps Intermediate Fields -> Canonical Staging Fields
    product_id: pid_extracted         # Map the properly extracted ID
    source_product_id: pid_extracted    # Map the same extracted ID here
    product_name: name                 # Direct map
    # brand:                           # Assumed not directly extracted, null (or parse from name downstream)
    price_current: price_val           # Direct map
    # price_regular:                   # Not extracted, will be null
    currency: '"EUR"'                  # Hardcode canonical 'currency'
    # price_unit_str:                  # Not extracted, will be null
    # unit_quantity_str:               # Not extracted (might be parsed from name downstream: "125 g"), will be null
    category_raw: category_slug        # Store the source category slug as raw category info
    # category_list:                   # Not extracted, will be null
    # category_l1:                     # Not extracted, will be null (parse category_raw downstream)
    # category_l2:                     # Not extracted, will be null
    # category_l3:                     # Not extracted, will be null
    product_url: url_main              # Direct map
    # product_urls_raw:                # Not extracted, will be null
    # image_urls:                      # Assumed not extracted in basic example, will be empty list []
    # source:                          # Added automatically by framework
    # scraped_timestamp:               # Added automatically by framework (using framework time, not source field)
    # is_available:                    # Not extracted, default will apply (e.g., true or null)
    attributes_raw:                    # Create a dictionary for extra attributes
        rating_text: rating_info       # Store the rating text/value if needed later