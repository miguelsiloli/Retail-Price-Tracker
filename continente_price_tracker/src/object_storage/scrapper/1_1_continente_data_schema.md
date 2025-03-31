# Source-to-Canonical Staging Schema Mappings

This document illustrates how intermediate fields extracted by the source-specific `parser_config` are mapped to the fields defined in the **Canonical Product Staging Schema Definition**. This mapping is defined within each source's configuration file (e.g., `config/source_name.yaml`) under the `schema_mapping` key.

---

## 1. Continente Mapping (`config/continente.yaml`)

**Assumed Intermediate Fields (Extracted by `parser_config`):**

*   `pid`: From `Product ID` column/selector
*   `name`: From `Product Name` column/selector
*   `current_price`: From `Price` column/selector (parsed as float)
*   `brand_name`: From `Brand` column/selector
*   `category_path`: From `Category` column/selector (e.g., "Bebé/Alimentação...")
*   `img_url`: From `Image URL` column/selector
*   `min_qty_str`: From `Minimum Quantity` column/selector (e.g., "1 un")
*   `link`: From `Product Link` column/selector
*   `source_category_id`: From `cgid` column/selector (e.g., "bebe")
*   `price_per_unit_raw`: From `Price per unit` column/selector (e.g., "€18,39/un")
*   `availability_status`: (Optional) Parsed boolean indicating stock, default `None`
*   `regular_price`: (Optional) Parsed original price if available, default `None`

**`schema_mapping` Section in `config/continente.yaml`:**

```yaml
schema_mapping: # Maps Intermediate Fields -> Canonical Staging Fields
    product_id: pid                 # Map intermediate 'pid' to canonical 'product_id'
    source_product_id: pid          # Map intermediate 'pid' also to canonical 'source_product_id'
    product_name: name              # Map intermediate 'name' to canonical 'product_name'
    brand: brand_name               # Map intermediate 'brand_name' to canonical 'brand'
    price_current: current_price    # Map intermediate 'current_price' to canonical 'price_current'
    price_regular: regular_price    # Map intermediate 'regular_price' (if extracted)
    currency: '"EUR"'               # Hardcode canonical 'currency' (Note quotes for literal string)
    price_unit_str: price_per_unit_raw # Map intermediate raw PPU string
    unit_quantity_str: min_qty_str # Map intermediate raw quantity string
    category_raw: category_path     # Map intermediate category path string
    # category_list:                # Not directly extracted, will be null
    # category_l1:                  # Not directly extracted, will be null
    # category_l2:                  # Not directly extracted, will be null
    # category_l3:                  # Not directly extracted, will be null
    product_url: link               # Map intermediate 'link' to canonical 'product_url'
    # product_urls_raw:             # Not extracted in this example, will be null
    image_urls: '[img_url]'         # Create a list containing the single image URL (Syntax depends on implementation, might need post-processing step or helper)
    # source:                       # Added automatically by framework
    # scraped_timestamp:            # Added automatically by framework
    is_available: availability_status # Map intermediate availability (if extracted)
    attributes_raw:                 # Create a dictionary for extra attributes
      source_cgid: source_category_id # Add the original cgid here
      # Add other minor fields here if needed