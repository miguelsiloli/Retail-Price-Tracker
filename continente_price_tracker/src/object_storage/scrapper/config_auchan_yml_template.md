# --- config/auchan.yaml (Example Enhanced V2) ---
source_name: auchan
enabled: true
fetcher_type: requests_html
# ... other fetcher, task, concurrency, output config ...

parser_config:
    product_container_selector: "div.product" # Selector for the repeating product element

    fields: # Define fields to extract FROM EACH product_container
        # --- Simple Field Example ---
        product_id: # Desired field name for this source's output dict
            selector: "[data-pid]" # CSS selector relative to product_container
            attribute: "data-pid" # Attribute to extract (optional, defaults to text if missing)
            type: str # Expected data type AFTER extraction
            mandatory: true # Must this field exist and have a non-null value?

        # --- Another Simple Field (Name) ---
        product_name:
            selector: "div.pdp-link a"
            # 'attribute' missing -> defaults to element's text content
            type: str
            mandatory: true
            post_processing: # Optional: Apply simple cleaning steps
                - { function: strip_whitespace }

        # --- Field requiring Type Conversion and Validation ---
        price:
            selector: "span.value[content]"
            attribute: "content"
            # Raw extracted value is likely string, we want float
            type: float # Target type for validation
            mandatory: false # Allow products without price
            validation_rules: # Specific rules beyond type/mandatory
                - { function: min_value, value: 0.0 }

        # --- Handling NESTED Data (e.g., JSON in an attribute) ---
        # Approach 1: Define fields extracted FROM the nested structure
        categories: # This will become a dictionary
            _source_selector: "div.product-tile" # Selector for the element containing the nested data attribute
            _source_attribute: "data-gtm-new" # The attribute holding the nested data (JSON string)
            _parse_as: json # How to interpret the attribute's content
            _subfields: # Define fields *within* the parsed JSON
                level_1: # Becomes categories['level_1']
                    path: "item_category" # JSON path or key
                    type: str
                    mandatory: false
                level_2: # Becomes categories['level_2']
                    path: "item_category2"
                    type: str
                    mandatory: false
                level_3: # Becomes categories['level_3']
                    path: "item_category3"
                    type: str
                    mandatory: false

        # Approach 2 (Alternative for Nested): Flatten directly if preferred
        category_l1_flat: # Direct field name in the output dict
            _source_selector: "div.product-tile[data-gtm-new]" # Combine selector and attribute check
            _source_attribute: "data-gtm-new"
            _parse_as: json
            path: "item_category" # Path within the parsed JSON
            type: str
            mandatory: false

        category_l2_flat:
            _source_selector: "div.product-tile[data-gtm-new]"
            _source_attribute: "data-gtm-new"
            _parse_as: json
            path: "item_category2"
            type: str
            mandatory: false

        # --- Field requiring Regex Extraction ---
        unit_info:
             selector: "span.product-unit-info" # Assume this contains text like "Approx. 1.5 kg"
             type: str # Keep raw string initially
             mandatory: false
             post_processing:
                 # Example: Extract number and unit using regex if needed later
                 - { function: extract_pattern, pattern: "([\d.]+)\s*(\w+)", output_fields: ["unit_quantity_extracted", "unit_measure_extracted"] }

        # --- Image URL Example ---
        image_main_url:
            selector: "div.image-container img"
            attribute: "src"
            type: str
            mandatory: false
            validation_rules:
                - { function: is_url } # Simple check if it looks like a URL

# --- Schema Mapping (Now simpler, just renaming/selection) ---
# Maps the field names defined above ('product_id', 'price', 'categories', 'category_l1_flat' etc.)
# to the *final* canonical schema field names.
# Only fields listed here will be included in the final output.
schema_mapping: # Maps fields parsed by THIS config -> CANONICAL fields
    id: product_id # Canonical 'id' comes from source's 'product_id'
    name: product_name
    price_current: price # Canonical 'price_current' comes from source's 'price'
    categories_structured: categories # Canonical 'categories_structured' dict comes from source's 'categories' dict
    category_l1: category_l1_flat # Or map the flattened version if preferred
    category_l2: category_l2_flat
    image_url: image_main_url # Assuming canonical schema only needs one URL
    # ... map other necessary fields ...
    # 'unit_info' might not be mapped directly if its components were extracted/mapped
    # unit_quantity: unit_quantity_extracted
    # unit_measure: unit_measure_extracted