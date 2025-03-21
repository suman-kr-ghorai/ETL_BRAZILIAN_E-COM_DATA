import pandas as pd

def generate_star_schema(df):
    # Dimension: Customers
    dim_customers = df[["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]].drop_duplicates().copy()
    dim_customers.insert(0, "customer_key", range(1, len(dim_customers) + 1))  # Surrogate Key
    
    # Dimension: Sellers
    dim_sellers = df[["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]].drop_duplicates().copy()
    dim_sellers.insert(0, "seller_key", range(1, len(dim_sellers) + 1))  # Surrogate Key

    # Dimension: Products
    dim_products = df[[
        "product_id", "product_category_name", "product_category_name_english",
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ]].drop_duplicates().copy()
    dim_products.insert(0, "product_key", range(1, len(dim_products) + 1))  # Surrogate Key

    # Dimension: Payment Types
    dim_payment_types = df[["payment_type"]].drop_duplicates().reset_index(drop=True)
    dim_payment_types.insert(0, "payment_type_key", range(1, len(dim_payment_types) + 1))  # Surrogate Key

    # Dimension: Reviews
    dim_reviews = df[["review_id", "order_id", "review_score"]].drop_duplicates().copy()
    dim_reviews.insert(0, "review_key", range(1, len(dim_reviews) + 1))  # Surrogate Key

    # Fact Table: Orders
    fact_orders = df[[
        "order_id", "customer_id", "seller_id", "product_id", "order_status",
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date", "shipping_limit_date",
        "payment_installments", "payment_value", "price", "freight_value", "payment_type"
    ]].copy()

    # Convert business keys to surrogate keys
    fact_orders = fact_orders.merge(dim_customers[["customer_id", "customer_key"]], on="customer_id", how="left")
    fact_orders = fact_orders.merge(dim_sellers[["seller_id", "seller_key"]], on="seller_id", how="left")
    fact_orders = fact_orders.merge(dim_products[["product_id", "product_key"]], on="product_id", how="left")
    fact_orders = fact_orders.merge(dim_payment_types[["payment_type", "payment_type_key"]], on="payment_type", how="left")

    # Drop business keys (keeping surrogate keys)
    fact_orders = fact_orders.drop(columns=["customer_id", "seller_id", "product_id", "payment_type"])

    return dim_customers, dim_sellers, dim_products, dim_payment_types, dim_reviews, fact_orders
