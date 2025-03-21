import pandas as pd

def merge_ecommerce_data(orders, customers, order_payments, order_reviews, order_items, products, category_translation, sellers):
    """
    Merges multiple e-commerce related DataFrames into a single DataFrame.
    
    Parameters:
    - orders: DataFrame containing order details
    - customers: DataFrame containing customer details
    - order_payments: DataFrame containing order payment details
    - order_reviews: DataFrame containing order review details
    - order_items: DataFrame containing order items details
    - products: DataFrame containing product details
    - category_translation: DataFrame containing product category translations
    - sellers: DataFrame containing seller details
    
    Returns:
    - Merged DataFrame
    """
    df = orders.merge(customers, on="customer_id", how="left")
    df = df.merge(order_payments, on="order_id", how="left")
    df = df.merge(order_reviews, on="order_id", how="left")
    df = df.merge(order_items, on="order_id", how="left")
    df = df.merge(products, on="product_id", how="left")
    df = df.merge(category_translation, on="product_category_name", how="left")
    df = df.merge(sellers, on="seller_id", how="left")
    df.to_csv("./data/merged.csv")
    
    return df
