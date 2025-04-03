from extract_from_s3 import extract_all_data



def transform_dim_customers(customers_df, geo_df):
    geo_unique = geo_df[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']].drop_duplicates('geolocation_zip_code_prefix')
    
    dim_customers = customers_df.merge(
        geo_unique, how='left',
        left_on='customer_zip_code_prefix',
        right_on='geolocation_zip_code_prefix'
        ).drop(columns='geolocation_zip_code_prefix').dropnna()
    
    return dim_customers[['customer_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state', 'geolocation_lat', 'geolocation_lng']]



def transform_dim_sellers(sellers_df, geo_df):
    geo_unique = geo_df[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']].drop_duplicates('geolocation_zip_code_prefix')
    
    dim_sellers = sellers_df.merge(
        geo_unique, how= 'left',
        left_on='seller_zip_code_prefix',
        right_on='geolocation_zip_code_prefix'
        ).drop(columns='geolocation_zip_code_prefix').dropna()
    
    return dim_sellers[['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state', 'geolocation_lat', 'geolocation_lng']]
    


def transform_dim_products(products_df,cat_df):
    dim_products = products_df.merge(
        cat_df, how='left',
        left_on='product_category_name'
    ).drop(columns=['product_category_name', 
           'product_name_lenght', 
           'product_description_lenght', 
           'product_photos_qty',
           'product_weight_g',
           'product_length_cm',
           'product_height_cm',
           'product_width_cm']).dropna()
    
    return dim_products[['product_id', 'product_category_name_english']]



def transform_dim_items(items_df, reviews_df):
    dim_items = items_df.merge(
        reviews_df, how='left',
        left_on = 'order_id'
    ).drop(columns=['review_answer_timestamp', 'shipping_limit_date']).dropna()
    
    return dim_items[['order_id', 'product_id', 
                      'seller_id', 'price', 
                      'freight_value', 'review_id', 
                      'review_score']]


    
def transform_dim_payments(payments_df):
    payments_df = payments_df.drop(columns=['payment_sequential']).dropna()
    
    return payments_df[['order_id', 'payment_type', 'payment_installments', 'payment_value']]



def transform_orders(orders_df):
    orders_df['order_purchase_timestamp'] = pd.to_datetime(orders_df['order_purchase_timestamp'])
    orders_df['order_delivered_customer_timestamp'] = pd.to_datetime(orders_df['order_delivered_customer_date'])
    
    orders_df = orders_df.drop(columns=[
        'order_delivered_carrier_date', 
        'order_estimated_delivery_date',
        'order_delivered_customer_date'])
    
    return orders_df[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp','order_delivered_customer_timestamp']]



def transform_fact_orders(orders_df, dim_customers_df, dim_sellers, dim_products, dim_items, dim_payments)
    clean_orders = transform_orders(orders_df).drop()
    clean_customers = dim_customers_df.drop(columns=['customer_zip_code_prefix', 'customer_state', 'geolocation_lat', 'geolocation_lng']).dropna()
    clean_sellers = dim_sellers.drop(columns=['seller_id']).dropna()
    clean_products = dim_products.dropna()
    clean_items = dim_items.drop(columns=['order_id','product_id''price', 'freight_value']).dropna()
    clean_payments = dim_payments.drop(columns=['order_id']).dropna()
    
    