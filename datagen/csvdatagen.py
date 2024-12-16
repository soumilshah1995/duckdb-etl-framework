import csv
import uuid
from io import StringIO
import boto3
from datetime import datetime, timedelta

def generate_static_customer_data():
    header = ['customer_id', 'name', 'email', 'address', 'phone']
    data = [
        ['C001', 'John Doe', 'john.doe@email.com', '123 Main St, City A', '555-0101'],
        ['C002', 'Jane Smith', 'jane.smith@email.com', '456 Oak Rd, City B', '555-0202'],
        ['C003', 'Bob Johnson', 'bob.johnson@email.com', '789 Pine Ave, City C', '555-0303'],
        ['C004', 'Alice Brown', 'alice.brown@email.com', '321 Elm St, City D', '555-0404'],
        ['C005', 'Charlie Davis', 'charlie.davis@email.com', '654 Maple Dr, City E', '555-0505']
    ]
    return [header] + data

def generate_static_order_data():
    header = ['order_id', 'customer_id', 'order_date', 'total_amount', 'status']
    data = [
        ['O001', 'C001', '2023-01-15', 100.50, 'Shipped'],
        ['O002', 'C002', '2023-02-20', 75.25, 'Delivered'],
        ['O003', 'C003', '2023-03-10', 200.00, 'Processing'],
        ['O004', 'C001', '2023-04-05', 50.75, 'Shipped'],
        ['O005', 'C004', '2023-05-12', 150.00, 'Delivered'],
        ['O006', 'C002', '2023-06-18', 80.50, 'Processing'],
        ['O007', 'C005', '2023-07-22', 120.25, 'Shipped'],
        ['O008', 'C003', '2023-08-30', 90.00, 'Delivered'],
        ['O009', 'C004', '2023-09-14', 180.75, 'Processing'],
        ['O010', 'C005', '2023-10-25', 60.50, 'Shipped']
    ]
    return [header] + data

def upload_to_s3(data, bucket, key):
    s3 = boto3.client('s3')
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerows(data)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"Uploaded {key} to S3 bucket {bucket}")

def main():
    bucket = 'XX'
    customer_key = f'raw/customers/customers_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    order_key = f'raw/orders/orders_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'

    customer_data = generate_static_customer_data()
    order_data = generate_static_order_data()

    upload_to_s3(customer_data, bucket, customer_key)
    upload_to_s3(order_data, bucket, order_key)

if __name__ == "__main__":
    main()
