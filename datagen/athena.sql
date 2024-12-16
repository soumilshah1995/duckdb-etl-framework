CREATE TABLE default.customers (
                                   customer_id STRING,
                                   name STRING,
                                   email STRING,
                                   address STRING,
                                   phone STRING
)
    LOCATION 's3://<bucket>/warehouse/customers'
TBLPROPERTIES (
'table_type' = 'ICEBERG',
'format' = 'PARQUET'
);

INSERT INTO default.customers (customer_id, name, email, address, phone) VALUES
                                                                             ('C001', 'John Doe', 'john.doe@email.com', '123 Main St, City A', '555-0101'),
                                                                             ('C002', 'Jane Smith', 'jane.smith@email.com', '456 Oak Rd, City B', '555-0202'),
                                                                             ('C003', 'Bob Johnson', 'bob.johnson@email.com', '789 Pine Ave, City C', '555-0303'),
                                                                             ('C004', 'Alice Brown', 'alice.brown@email.com', '321 Elm St, City D', '555-0404'),
                                                                             ('C005', 'Charlie Davis', 'charlie.davis@email.com', '654 Maple Dr, City E', '555-0505');


CREATE TABLE default.orders (
                                order_id STRING,
                                customer_id STRING,
                                order_date DATE,
                                total_amount DOUBLE,
                                status STRING
)
    LOCATION 's3://<bucket>/warehouse/orders'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET'
);


INSERT INTO default.orders (order_id, customer_id, order_date, total_amount, status) VALUES
                                                                                         ('O001', 'C001', DATE '2023-01-15', 100.50, 'Shipped'),
                                                                                         ('O002', 'C002', DATE '2023-02-20', 75.25, 'Delivered'),
                                                                                         ('O003', 'C003', DATE '2023-03-10', 200.00, 'Processing'),
                                                                                         ('O004', 'C001', DATE '2023-04-05', 50.75, 'Shipped'),
                                                                                         ('O005', 'C004', DATE '2023-05-12', 150.00, 'Delivered'),
                                                                                         ('O006', 'C002', DATE '2023-06-18', 80.50, 'Processing'),
                                                                                         ('O007', 'C005', DATE '2023-07-22', 120.25, 'Shipped'),
                                                                                         ('O008', 'C003', DATE '2023-08-30', 90.00, 'Delivered'),
                                                                                         ('O009', 'C004', DATE '2023-09-14', 180.75, 'Processing'),
                                                                                         ('O010', 'C005', DATE '2023-10-25', 60.50, 'Shipped');

