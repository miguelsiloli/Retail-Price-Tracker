schema_mapping: # Maps Intermediate Fields -> Canonical Staging Fields
    product_id: product_id           # Direct map
    source_product_id: product_id    # Direct map (assuming same ID used)
    product_name: product_name         # Direct map
    # brand:                         # Assumed not directly extracted, will be null (or parsed from name downstream)
    price_current: price             # Direct map
    # price_regular:                 # Assumed not directly extracted, will be null
    currency: '"EUR"'                # Hardcode canonical 'currency'
    # price_unit_str:                # Assumed not directly extracted, will be null
    # unit_quantity_str:             # Assumed not directly extracted, will be null
    category_raw: category_info.raw_json_str # Store the original raw JSON string of categories
    # category_list:                 # Not extracted as a simple list, will be null
    category_l1: category_info.level_1 # Map L1 from parsed category dict
    category_l2: category_info.level_2 # Map L2 from parsed category dict
    category_l3: category_info.level_3 # Map L3 from parsed category dict
    product_url: product_main_url    # Map the primary URL (assuming it was extracted separately, maybe from url_data_json)
    product_urls_raw: url_data_json    # Map the raw JSON string containing all URLs
    image_urls: '[image_url_main]'     # Create a list containing the single image URL
    # source:                        # Added automatically by framework
    # scraped_timestamp:             # Added automatically by framework
    # is_available:                  # Assumed not directly extracted, default will apply
    attributes_raw:                  # Create a dictionary for extra attributes
        bv_rating_id: rating_id      # Store BazaarVoice ID if needed
        labels: labels_json          # Store labels JSON string
        promotion_text: promo_text   # Store promotion text