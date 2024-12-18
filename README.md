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

# Iceberg Tables
![image](https://github.com/user-attachments/assets/3aa5623b-9ad1-43d9-a9a6-925b89ec4c25)


# CSV File Processing 
![image](https://github.com/user-attachments/assets/7999dd0c-b25e-4e36-97b1-a9004d03f87b)


#### Run template 
```
python3 template.py
```

Contribution
We welcome contributions to enhance this framework! If you'd like to contribute, feel free to fork the repository, make improvements, and submit a merge request (MR) with your changes. Whether you're adding new features, fixing bugs, or improving the documentation, your help is greatly appreciated. Please ensure that your changes are well-tested, and don't forget to update the documentation if necessary. We look forward to your contributions and thank you for helping make this project better!
