import streamlit as st
import logging
from utils.kaggle_utils import fetch_data
from utils.mysql_fetch_utils import fetch_table
from utils.convert_mysql_dtypes import convert_mysql_dtypes 
from utils.merge_df import merge_ecommerce_data
from utils.cleaner_utils import clean
from utils.convert_dtypes import convert_dtypes
import pandas as pd

def main():
    st.title("ETL-P")
    
    if "data_fetched" not in st.session_state:
        st.session_state.data_fetched = False
    
    if "data_merged" not in st.session_state:
        st.session_state.data_merged = False
    
    if "data_cleaned" not in st.session_state:
        st.session_state.data_cleaned = False
    
    if "data_converted" not in st.session_state:
        st.session_state.data_converted = False
    
    if not st.session_state.data_fetched:
        if st.button("Fetch Data from Kaggle"):
            try:
                fetch_data()
                st.session_state.data_fetched = True
                logging.info("Data fetching completed successfully.")
            except Exception as e:
                st.error(f"Error fetching data: {e}")
                logging.error(f"Error fetching data: {e}")
    
    if st.session_state.data_fetched:
        st.success("Data fetched successfully!")

    try:
        # Fetching tables from MySQL and converting data types
        orders = convert_mysql_dtypes(fetch_table("olist_orders_dataset"))
        products = convert_mysql_dtypes(fetch_table("olist_products_dataset"))
        sellers = convert_mysql_dtypes(fetch_table("olist_sellers_dataset"))
        category_translation = convert_mysql_dtypes(fetch_table("product_category_name_translation"))
        customers = pd.read_csv(r"data/olist_customers_dataset.csv")
        order_items = pd.read_csv(r"data/olist_order_items_dataset.csv")
        order_payments = pd.read_csv(r"data/olist_order_payments_dataset.csv")
        order_reviews = pd.read_csv(r"data/olist_order_reviews_dataset.csv")
    
    except Exception as e:
        st.error(f"Error fetching data from MySQL: {e}")
        logging.error(f"Error fetching data from MySQL: {e}")
        return
    
    if st.session_state.data_fetched and not st.session_state.data_merged:
        if st.button("Merge Data"):
            try:
                merged_df = merge_ecommerce_data(orders, customers, order_payments, order_reviews, order_items, products, category_translation, sellers)
                st.session_state.data_merged = True
                st.session_state.merged_df = merged_df
                st.success("Data merged successfully!")
            except Exception as e:
                st.error(f"Error merging data: {e}")
                logging.error(f"Error merging data: {e}")
    
    if st.session_state.data_merged and not st.session_state.data_cleaned:
        if st.button("Clean Data"):
            try:
                cleaned_df = clean(st.session_state.merged_df)
                st.session_state.data_cleaned = True
                st.session_state.cleaned_df = cleaned_df
                st.success("Data cleaned successfully!")
            except Exception as e:
                st.error(f"Error cleaning data: {e}")
                logging.error(f"Error cleaning data: {e}")
    
    if st.session_state.data_cleaned and not st.session_state.data_converted:
        if st.button("Convert Data Types"):
            try:
                converted_df = convert_dtypes(st.session_state.cleaned_df)
                st.session_state.data_converted = True
                st.session_state.converted_df = converted_df
                st.success("Data types converted successfully!")
            except Exception as e:
                st.error(f"Error converting data types: {e}")
                logging.error(f"Error converting data types: {e}")
    
    if st.session_state.data_converted:
        cleaned_df = st.session_state.converted_df
        st.write("### Cleaned Data Preview")
        st.write(cleaned_df.head())
        
        st.write("### Null Values Count Before Cleaning")
        st.write(st.session_state.merged_df.isnull().sum())
        
        st.write("### Null Values Count After Cleaning")
        st.write(cleaned_df.isnull().sum())
        
        st.write("### Data Shape Before Cleaning")
        st.write(st.session_state.merged_df.shape)
        
        st.write("### Data Shape After Cleaning")
        st.write(cleaned_df.shape)

if __name__ == "__main__":
    main()
