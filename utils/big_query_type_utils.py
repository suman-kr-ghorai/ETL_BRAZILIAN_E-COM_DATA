import pandas as pd

def generate_star_schema(df):
    # Creating dimension tables with unique values
    dim_customers = df[["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]].drop_duplicates().copy()
    dim_sellers = df[["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]].drop_duplicates().copy()
    dim_products = df[[
        "product_id", "product_category_name", "product_category_name_english",
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ]].drop_duplicates().copy()
    dim_payment_types = df[["payment_type"]].drop_duplicates().reset_index(drop=True)
    dim_reviews = df[["review_id", "order_id", "review_score"]].drop_duplicates().copy()

    # Adding surrogate keys (Primary Keys)
    dim_customers["customer_key"] = dim_customers.index.astype(str)
    dim_sellers["seller_key"] = dim_sellers.index.astype(str)
    dim_products["product_key"] = dim_products.index.astype(str)
    dim_payment_types["payment_type_id"] = dim_payment_types.index.astype(str)
    dim_reviews["review_key"] = dim_reviews.index.astype(str)

    # Convert integer zip codes to STRING (BigQuery does not support nullable int64 well)
    dim_customers["customer_zip_code_prefix"] = dim_customers["customer_zip_code_prefix"].astype(str)
    dim_sellers["seller_zip_code_prefix"] = dim_sellers["seller_zip_code_prefix"].astype(str)

    # Convert float columns explicitly to float64
    dim_products["product_name_length"] = dim_products["product_name_length"].astype("float64")
    dim_reviews["review_score"] = dim_reviews["review_score"].astype("float64")

    # Fact table (orders)
    fact_orders = df[[
        "order_id", "customer_id", "seller_id", "product_id", "order_status",
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date", "shipping_limit_date",
        "payment_installments", "payment_value", "price", "freight_value", "payment_type"
    ]].copy()

    # Convert categorical fields to string
    fact_orders["order_status"] = fact_orders["order_status"].astype(str)
    fact_orders["payment_type"] = fact_orders["payment_type"].astype(str)

    # Convert datetime columns to actual timestamps
    fact_orders["order_purchase_timestamp"] = pd.to_datetime(fact_orders["order_purchase_timestamp"])
    fact_orders["order_approved_at"] = pd.to_datetime(fact_orders["order_approved_at"])
    fact_orders["order_delivered_carrier_date"] = pd.to_datetime(fact_orders["order_delivered_carrier_date"])
    fact_orders["order_delivered_customer_date"] = pd.to_datetime(fact_orders["order_delivered_customer_date"])
    fact_orders["order_estimated_delivery_date"] = pd.to_datetime(fact_orders["order_estimated_delivery_date"])
    fact_orders["shipping_limit_date"] = pd.to_datetime(fact_orders["shipping_limit_date"])

    # Replace business keys with surrogate keys in fact_orders
    fact_orders = fact_orders.merge(dim_customers[['customer_id', 'customer_key']], on="customer_id", how="left")
    fact_orders = fact_orders.merge(dim_sellers[['seller_id', 'seller_key']], on="seller_id", how="left")
    fact_orders = fact_orders.merge(dim_products[['product_id', 'product_key']], on="product_id", how="left")

    # Drop old business keys
    fact_orders.drop(columns=["customer_id", "seller_id", "product_id"], inplace=True)

    return dim_customers, dim_sellers, dim_products, dim_payment_types, dim_reviews, fact_orders
