import pandas as pd

def clean(df):
    """
    Cleans the merged e-commerce DataFrame by handling missing values, renaming columns, and filling timestamps.
    
    Parameters:
    - df: Merged DataFrame
    
    Returns:
    - Cleaned DataFrame
    """
    # Handle missing reviews
    df['review_comment_title'] = df['review_comment_title'].fillna("Not Available")
    df['review_comment_message'] = df['review_comment_message'].fillna("Not Available")
    
    # Fill missing review scores with mean
    df['review_score'] = df['review_score'].fillna(df['review_score'].mean())
    
    # Fill review creation date with delivery date
    df['review_creation_date'] = df['review_creation_date'].fillna(df['order_delivered_customer_date'].where(df['order_delivered_customer_date'].notna()))
    
    # Replace NaN values with "Unknown" for categorical columns
    df[['payment_type', 'product_category_name', 'product_category_name_english']] = \
        df[['payment_type', 'product_category_name', 'product_category_name_english']].fillna("Unknown")
    
    # Replace NaN values with median values for numerical columns
    num_cols = [
        'payment_sequential', 'payment_installments', 'order_item_id', 'price', 'freight_value', 
        'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'
    ]
    df[num_cols] = df[num_cols].apply(lambda x: x.fillna(x.median()), axis=0)
    
    # Rename misspelled columns
    df = df.rename(columns={
        'product_name_lenght': 'product_name_length',
        'product_description_lenght': 'product_description_length'
    })
    
    # Fill timestamps with relevant values
    df['order_approved_at'] = df['order_approved_at'].fillna(df['order_purchase_timestamp'])
    df['order_delivered_carrier_date'] = df['order_delivered_carrier_date'].fillna(df['order_approved_at'])
    df['order_delivered_customer_date'] = df['order_delivered_customer_date'].fillna(df['order_estimated_delivery_date'])
    
    # Drop remaining null values (~less than 5%)
    df = df.dropna()
    
    return df
