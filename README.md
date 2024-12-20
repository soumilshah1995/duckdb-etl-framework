## DuckDB-Powered Lightweight ETL: An Extensible Framework for Seamless Data Integration


![image](https://github.com/user-attachments/assets/02c932f6-66d2-496c-98ce-41da507c8c15)


This repository provides a lightweight ETL framework powered by **DuckDB**, designed for seamless data integration. With this framework, you can easily **extract**, **transform**, and **load** data from various sources into your data lake or warehouse. The architecture allows for extensibility, enabling users to integrate with Lakehouse formats like **Iceberg** and implement custom transformation logic as needed.


## Features

- **Lightweight**: Utilizes DuckDB for in-memory processing, making it efficient and fast.
- **Extensible**: Easily extend functionality to support various data sources and formats, including Lakehouse architectures like Iceberg.
- **Custom Logic**: Implement your own transformation logic to meet specific business requirements.

## Getting Started

### Step 1: Install Dependencies


Before you begin, you'll need to set up your AWS credentials and install the required dependencies.

```
export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
export AWS_REGION="us-east-1"

```
Install Python dependencies:
```
pip3 install -r requirements.txt
```

Step 2: Define the Configuration File (config.yaml)


#### CSV Files Example
```
# DuckDB Configuration
duckdb:
  path: default.duckdb
  extension:
    - name: httpfs  # HTTP File System extension
    - name: aws     # AWS S3 extension

# Input Tables Configuration
input:
  tables:
    - name: customers
      path: s3://<masked_bucket>/raw/customers/*.csv
      format: csv
      mode: full
      checkpoint_path: 's3://<masked_bucket>/checkpoints/customers_checkpoint.json'
    - name: orders
      path: s3://<masked_bucket>/raw/orders/
      format: csv
      mode: inc
      checkpoint_path: 's3://<masked_bucket>/checkpoints/orders_checkpoint.json'

# Transformation SQL Query
transform:
  sql: |
    SELECT 
      c.customer_id, 
      c.name, 
      o.order_id, 
      o.order_date, 
      o.total_amount 
    FROM 
      customers c 
    JOIN 
      orders o ON c.customer_id = o.customer_id;

# Output Configuration
output:
  path: 's3://<bucket>/output/csv/'
  format: csv
  mode: overwrite

```
#### Iceberg Tables Example

```
# DuckDB Configuration
duckdb:
  path: mydatabase.duckdb
  extension:
    - name: httpfs  # HTTP File System extension
    - name: aws     # AWS S3 extension
    - name: iceberg # Iceberg extension

# Input Tables Configuration
input:
  tables:
    - name: customers
      path: 's3://<masked_bucket>/warehouse/customers/'
      format: iceberg   # Changed to iceberg format
      mode: full

    - name: orders
      path: 's3://<masked_bucket>/warehouse/orders/'
      format: iceberg   # Changed to iceberg format
      mode: full

# Transformation SQL Query (Adjust as needed based on your schema)
transform:
  sql: |
    SELECT 
      c.customer_id, 
      c.name, 
      o.order_id, 
      o.order_date, 
      o.total_amount 
    FROM 
      customers c 
    JOIN 
      orders o ON c.customer_id = o.customer_id;

# Output Configuration
output:
  path: 's3://<masked_bucket>/icebergoutput/csv/'
  format: csv
  mode: overwrite
  threshold: 2

```
### File Splitter Example
```
duckdb:
  path: mydatabase.duckdb
  extension:
    - name: httpfs
input:
  tables:
    - name: nyctaxi
      path: '/<masked_path>/output/*.csv'
      format: csv
      mode: full
transform:
  sql: |
    SELECT 
      * 
    FROM 
      nyctaxi
output:
  path: '/<masked_path>/transformed_output'
  format: csv
  mode: overwrite
  threshold: 10000000

```


#### Step 3: Running the Script
This repository provides a template script, template.py, that can be used with various configuration files for data processing operations. The script supports both local and AWS S3-based configuration files.


Run with Local Configurations
```
python3 template.py --config /path/to/config.yaml

```
Run with S3-based Configurations

## How to Use

Upload your config.yaml to an S3 bucket:
```
aws s3 cp /localpath/config.yaml s3://bucket/configs/config.yaml
```

Run the script using the S3 path:
```
python3 template.py --config s3://bucket/configs/config.yaml

```

Docker Setup

You can also build and run the framework using Docker for easier deployment.
Build the Docker Image

```
export AWS_ACCESS_KEY_ID="XX"
export AWS_SECRET_ACCESS_KEY="XX"
export AWS_DEFAULT_REGION="us-east-1"

docker build -t my-data-processor \
  --build-arg AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  --build-arg AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  --build-arg AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION .

```
Run the Docker Container
```
docker run -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
           -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
           -e AWS_DEFAULT_REGION=us-east-1 \
           my-data-processor python template.py --config s3://soumil-dev-bucket-1995/configs/csv.yaml

```


## Contribution


We welcome contributions to enhance this framework! If you'd like to contribute, feel free to fork the repository, make improvements, and submit a merge request (MR) with your changes. Whether you're adding new features, fixing bugs, or improving the documentation, your help is greatly appreciated. Please ensure that your changes are well-tested, and don't forget to update the documentation if necessary. We look forward to your contributions and thank you for helping make this project better!
