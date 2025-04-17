-- Clear staging tables to avoid duplication
TRUNCATE stg_customers;
TRUNCATE stg_geolocation;
TRUNCATE stg_order_items;
TRUNCATE stg_order_payments;
TRUNCATE stg_order_reviews;
TRUNCATE stg_orders;
TRUNCATE stg_products;
TRUNCATE stg_sellers;
TRUNCATE stg_product_category_name_translation;

-- COPY raw CSVs from S3 to staging tables in Redshift

COPY stg_customers
FROM 's3://ecom-sales-bucket/olist_data/olist_customers_dataset.csv'
IAM_ROLE 'arn:aws:iam::329599652266:role/RedshiftS3AccessRole'
FORMAT AS CSV
IGNOREHEADER 1
NULL AS ''
REGION 'eu-west-2';

COPY stg_geolocation
FROM 's3://ecom-sales-bucket/olist_data/olist_geolocation_dataset.csv'
IAM_ROLE 'arn:aws:iam::329599652266:role/RedshiftS3AccessRole'
FORMAT AS CSV
IGNOREHEADER 1
NULL AS ''
REGION 'eu-west-2';

COPY stg_order_items
FROM 's3://ecom-sales-bucket/olist_data/olist_order_items_dataset.csv'
IAM_ROLE 'arn:aws:iam::329599652266:role/RedshiftS3AccessRole'
FORMAT AS CSV
IGNOREHEADER 1
NULL AS ''
REGION 'eu-west-2';

COPY stg_order_payments
FROM 's3://ecom-sales-bucket/olist_data/olist_order_payments_dataset.csv'
IAM_ROLE 'arn:aws:iam::329599652266:role/RedshiftS3AccessRole'
FORMAT AS CSV
IGNOREHEADER 1
NULL AS ''
REGION 'eu-west-2';

COPY stg_order_reviews
FROM 's3://ecom-sales-bucket/olist_data/olist_order_reviews_dataset.csv'
IAM_ROLE 'arn:aws:iam::329599652266:role/RedshiftS3AccessRole' 
FORMAT AS CSV
IGNOREHEADER 1
NULL AS ''
REGION 'eu-west-2';

COPY stg_orders
FROM 's3://ecom-sales-bucket/olist_data/olist_orders_dataset.csv'
IAM_ROLE 'arn:aws:iam::329599652266:role/RedshiftS3AccessRole'
FORMAT AS CSV
IGNOREHEADER 1
NULL AS ''
REGION 'eu-west-2';

COPY stg_products
FROM 's3://ecom-sales-bucket/olist_data/olist_products_dataset.csv'
IAM_ROLE 'arn:aws:iam::329599652266:role/RedshiftS3AccessRole'
FORMAT AS CSV
IGNOREHEADER 1
NULL AS ''
REGION 'eu-west-2';

COPY stg_sellers
FROM 's3://ecom-sales-bucket/olist_data/olist_sellers_dataset.csv'
IAM_ROLE 'arn:aws:iam::329599652266:role/RedshiftS3AccessRole'
FORMAT AS CSV
IGNOREHEADER 1
NULL AS ''
REGION 'eu-west-2';

COPY stg_product_category_name_translation
FROM 's3://ecom-sales-bucket/olist_data/product_category_name_translation.csv'
IAM_ROLE 'arn:aws:iam::329599652266:role/RedshiftS3AccessRole'
FORMAT AS CSV
IGNOREHEADER 1
NULL AS ''
REGION 'eu-west-2';


