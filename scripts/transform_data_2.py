from scripts.extract_from_s3_1 import extract_all_data # Import the function to extract data from S3, this is called at the end of the script
import pandas as pd
import numpy as np



def transform_dim_customers(customers_df, geo_df):
    print("Transforming dim_customers...")
    geo_unique = geo_df[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']].drop_duplicates('geolocation_zip_code_prefix') # Extract unique geolocation data
    
    dim_customers = customers_df.merge(     # dim_customers is created by merging customers_df with geo_unique on the zip_code_prefix column 
        geo_unique, how='left',
        left_on='customer_zip_code_prefix',
        right_on='geolocation_zip_code_prefix'
        ).drop(columns='geolocation_zip_code_prefix').dropna()
    
    print("dim_customers transformed successfully!")
    
    return dim_customers[['customer_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state', 'geolocation_lat', 'geolocation_lng']] # Return the relevant columns



def transform_dim_sellers(sellers_df, geo_df):
    print("Transforming dim_sellers...")
    geo_unique = geo_df[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']].drop_duplicates('geolocation_zip_code_prefix') # Extract unique geolocation data
    
    dim_sellers = sellers_df.merge(     # dim_sellers is created by merging sellers_df with geo_unique on the zip_code_prefix column    
        geo_unique, how= 'left',
        left_on='seller_zip_code_prefix',
        right_on='geolocation_zip_code_prefix'
        ).drop(columns='geolocation_zip_code_prefix').dropna()
    
    print("dim_sellers transformed successfully!")
    
    return dim_sellers[['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state', 'geolocation_lat', 'geolocation_lng']] # Return the relevant columns
    


def transform_dim_products(products_df,cat_df): 
    print("Transforming dim_products...")
    
    dim_products = products_df.merge(       # dim_products is created by merging products_df with cat_df on the product_category_name column
        cat_df, how='left',
        left_on='product_category_name',
        right_on='product_category_name'
    ).drop(columns=['product_category_name', 
           'product_name_lenght', 
           'product_description_lenght', 
           'product_photos_qty',
           'product_weight_g',
           'product_length_cm',
           'product_height_cm',
           'product_width_cm']).dropna()
    
    print("dim_products transformed successfully!")
    
    return dim_products[['product_id', 'product_category_name_english']] # Return the relevant columns



def transform_dim_items(items_df, reviews_df):
    print("Transforming dim_items...")
    
    dim_items = items_df.merge(                   # dim_items is created by merging items_df with reviews_df on the order_id column
        reviews_df, how='left',
        left_on = 'order_id',
        right_on = 'order_id'
    ).drop(columns=['review_answer_timestamp', 'shipping_limit_date']).dropna()
    
    print("dim_items transformed successfully!")
    
    return dim_items[['order_id', 'product_id', 
                      'seller_id', 'price', 
                      'freight_value', 'review_id', 
                      'review_score']]             # Return the relevant columns


    
def transform_dim_payments(payments_df):
    print("Transforming dim_payments...")
    
    payments_df = payments_df.drop(columns=['payment_sequential']).dropna()                     # Drop the payment_sequential column and any rows with NaN values
    
    print("dim_payments transformed successfully!")
    return payments_df[['order_id', 'payment_type', 'payment_installments', 'payment_value']]   # Return the relevant columns



def transform_orders(orders_df):
    print("Transforming orders...")
    
    # Convert timestamps to datetime
    orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])
    orders_df['order_delivered_customer_timestamp'] = pd.to_datetime(orders_df['order_delivered_customer_date'])
    
    # Format to match Redshift requirements
    orders_df['order_purchase_timestamp'] = orders_df['order_purchase_timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
    orders_df['order_delivered_customer_timestamp'] = orders_df['order_delivered_customer_timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Drop unnecessary columns
    orders_df = orders_df.drop(columns=[
        'order_delivered_carrier_date', 
        'order_estimated_delivery_date',
        'order_delivered_customer_date'
    ])
    
    print("Orders transformed successfully!")
    
    return orders_df[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_delivered_customer_timestamp']]

    
    
def transform_fact_orders(orders_df, dim_customers, dim_sellers, dim_products, dim_items, dim_payments):
    print("Transforming fact_orders...")
    
    # Cleaning the data by dropping rows with NaN values in the relevant columns
    orders_clean = orders_df[['order_id', 'customer_id', 'order_purchase_timestamp']].dropna()
    clean_customers = dim_customers[['customer_id', 'customer_city']].dropna()
    clean_sellers = dim_sellers[['seller_id', 'seller_city']].dropna()
    clean_products = dim_products[['product_id', 'product_category_name_english']].dropna()
    clean_items = dim_items[['order_id', 'product_id', 'seller_id', 'price', 'freight_value']].dropna()
    clean_payments = dim_payments[['order_id', 'payment_type', 'payment_installments', 'payment_value']].dropna()   

    # Merging the cleaned dataframes to create the fact_orders dataframe
    fact_orders = orders_clean \
        .merge(clean_customers, on='customer_id', how='inner') \
        .merge(clean_items, on='order_id', how='left') \
        .merge(clean_sellers, on='seller_id', how='left') \
        .merge(clean_products, on='product_id', how='left') \
        .merge(clean_payments, on='order_id', how='left') \
        .dropna()

    print("fact_orders transformed successfully!")
    return fact_orders # Return the final fact_orders dataframe



if __name__ == "__main__":
    data = extract_all_data()

    dim_customers = transform_dim_customers(data['olist_customers_dataset'], data['olist_geolocation_dataset'])
    dim_sellers = transform_dim_sellers(data['olist_sellers_dataset'], data['olist_geolocation_dataset'])
    dim_products = transform_dim_products(data['olist_products_dataset'], data['product_category_name_translation'])
    dim_items = transform_dim_items(data['olist_order_items_dataset'], data['olist_order_reviews_dataset'])
    dim_payments = transform_dim_payments(data['olist_order_payments_dataset'])
    orders_df = transform_orders(data['olist_orders_dataset'])
    
    fact_orders = transform_fact_orders(
        orders_df, dim_customers, dim_sellers, dim_products, dim_items, dim_payments
    )

    
    # print("Transformed DataFrames:")
    #print("Fact Orders:")
    #print(fact_orders.head())
    #print("📊 Columns in fact_orders:", fact_orders.columns.tolist())
    # print("Dim customers:")
    # print(dim_customers.head())
    # print("Dim sellers:")
    # print(dim_sellers.head())
    # print("Dim products:")
    # print(dim_products.head())
    # print("Dim items:")
    # print(dim_items.head())
    # print("Dim payments:")
    # print(dim_payments.head())
    