from continente.preprocessing import *

def preprocess_and_insert_data_continente(df, db_interface):
    # Step 1: Read data from the Parquet file
    # df = pd.read_parquet(parquet_file)
    
    # Drop duplicates for the same product_id, source and timestamp
    df = df.drop_duplicates(subset=['Product ID', 'source', 'tracking_date'])
    df["tracking_date"] = df["tracking_date"].apply(to_unix_time)
    
    # Step 2: Apply preprocessing functions for each table
    df_product = product_table(df)
    df_category = category_table(df)  
    df_product_category = category_hierarchy_table(df)
    df_pricing = product_product_pricing(df)  
    
    # Step 3: Insert into respective tables using the new bulk insert methods
    # Insert products and retrieve product_id_pk
    product_ids_pk = db_interface.bulk_insert_into_product_table(df_product)
    
    # Insert category hierarchy and retrieve category_ids
    category_ids = db_interface.bulk_insert_into_category_hierarchy_table_with_defaults(df_category)
    
    # Ensure the product_id_pk is mapped correctly in the product_category dataframe
    df_product_category['product_id_pk'] = product_ids_pk
   
    # Insert product-category relationships
    db_interface.bulk_insert_into_product_category_table(df_product_category, product_ids_pk, category_ids)
   
    # Insert product pricing
    db_interface.bulk_insert_into_product_pricing_table(df_pricing, product_ids_pk)