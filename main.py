import pandas as pd
import boto3
import configparser

# Grab config
config = configparser.ConfigParser()
config.read('config.ini')

# Load data from CSV files
sales_df = pd.read_csv('sales.csv')
products_df = pd.read_csv('products.csv')
customers_df = pd.read_csv('customers.csv')

# Merge sales with products and customers
merged_df = sales_df.merge(products_df, on='product_id').merge(customers_df, on='customer_id')

# Select relevant columns
result_df = merged_df[['sale_id', 'first_name', 'last_name', 'product_name', 'sale_date', 'quantity', 'price']]

# Anonymize customer information
result_df['first_name'] = result_df['first_name'].apply(lambda x: 'Anonymous')
result_df['last_name'] = result_df['last_name'].apply(lambda x: 'Customer')

# Generate report
result_df.to_csv('sales_report.csv', index=False)

# Grab AWS config
access_key = config.get('aws', 'access_key')
secret_access_key = config.get('aws', 'secret_access_key')
bucket_name = config.get('aws', 'bucket_name')

# Upload the CSV file
s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
s3_bucket_name = bucket_name

s3.upload_file('sales_report.csv', s3_bucket_name, 'sales_report.csv')
