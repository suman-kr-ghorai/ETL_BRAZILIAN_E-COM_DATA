import pandas as pd
def generate_star_schema(df):
    dim_customers = df[["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]].drop_duplicates().copy()
    dim_sellers = df[["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]].drop_duplicates().copy()
    dim_products = df[[
        "product_id", "product_category_name", "product_category_name_english",
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ]].drop_duplicates().copy()
    dim_payment_types = df[["payment_type"]].drop_duplicates().reset_index(drop=True)
    dim_payment_types["payment_type_id"] = dim_payment_types.index.astype(str)
    dim_reviews = df[["review_id", "order_id", "review_score"]].drop_duplicates().copy()
    fact_orders = df[[
        "order_id", "customer_id", "seller_id", "product_id", "order_status",
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date", "shipping_limit_date",
        "payment_installments", "payment_value", "price", "freight_value", "payment_type"
    ]].copy()
    fact_orders["order_status"] = fact_orders["order_status"].astype(str)
    fact_orders["payment_type"] = fact_orders["payment_type"].astype(str)

    return dim_customers, dim_sellers, dim_products, dim_payment_types, dim_reviews, fact_orders