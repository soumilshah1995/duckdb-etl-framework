## DuckDB-Powered Lightweight ETL: An Extensible Framework for Seamless Data Integration


This repository provides a lightweight ETL framework powered by DuckDB, designed for seamless data integration. With this framework, you can easily extract, transform, and load data from various sources into your data lake or warehouse. The architecture allows for extensibility, enabling users to integrate with Lakehouse formats like Iceberg and implement custom logic as needed.
Features


Lightweight: Utilizes DuckDB for in-memory processing, making it efficient and fast.


Extensible: Easily extend functionality to support various data sources and formats, including Lakehouse architectures like Iceberg.


Custom Logic: Implement your own transformation logic to meet specific business requirements.

#### Step 1
```
export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
export AWS_REGION="us-east-1"
```

#### Define config.yaml

![image](https://github.com/user-attachments/assets/7999dd0c-b25e-4e36-97b1-a9004d03f87b)


#### Run template 
```
python3 template.py
```
