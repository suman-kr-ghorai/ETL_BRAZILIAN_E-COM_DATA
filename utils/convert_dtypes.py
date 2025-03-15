import pandas as pd

def convert_dtypes(df):
    """
    Converts data types of the merged DataFrame for optimized memory usage and faster processing.

    Parameters:
    - df: Merged DataFrame

    Returns:
    - DataFrame with updated data types
    """
    
    dtype_mapping = {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "category",
        "order_purchase_timestamp": "datetime64[ns]",
        "order_approved_at": "datetime64[ns]",
        "order_delivered_carrier_date": "datetime64[ns]",
        "order_delivered_customer_date": "datetime64[ns]",
        "order_estimated_delivery_date": "datetime64[ns]",
        "customer_unique_id": "string",
        "customer_zip_code_prefix": "Int32",
        "customer_city": "string",
        "customer_state": "category",
        "payment_sequential": "Int32",
        "payment_type": "category",
        "payment_installments": "Int32",
        "payment_value": "float32",
        "review_id": "string",
        "review_score": "float32",
        "order_item_id": "Int32",
        "product_id": "string",
        "seller_id": "string",
        "shipping_limit_date": "datetime64[ns]",
        "price": "float32",
        "freight_value": "float32",
        "product_category_name": "category",
        "product_name_length": "float32",
        "product_description_length": "float32",
        "product_photos_qty": "float32",
        "product_weight_g": "float32",
        "product_length_cm": "float32",
        "product_height_cm": "float32",
        "product_width_cm": "float32",
        "product_category_name_english": "category",
        "seller_zip_code_prefix": "Int32",
        "seller_city": "string",
        "seller_state": "category",
    }

    # Convert columns safely
    for col, dtype in dtype_mapping.items():
        if col in df.columns:
            if "datetime" in str(dtype):
                df[col] = pd.to_datetime(df[col], errors='coerce')  # Convert dates, set errors to NaT
            elif dtype == "Int32":
                df[col] = pd.to_numeric(df[col], errors="coerce", downcast="integer").astype("Int32")
            else:
                df[col] = df[col].astype(dtype)

    return df
