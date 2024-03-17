# Read the relevant libraries
library(RSQLite)
library(readr)

# Specify the database file name
db_file <- "database/database.db"

# Create the db connection with SQLite
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),dbname = db_file)
print(my_connection)

# Delete if tables already exist
#drop_tables <- "DROP TABLE IF EXISTS SUPPLIERS, CUSTOMERS, PRODUCTS, ADS, WAREHOUSE, CATEGORY, ORDER_DETAILS, ORDERS_PAYMENT, SUPPLY, STORE, ORDERS;"

# Entity Table Creation

# Create table for SUPPLIERS entity
sql_suppliers <- "
CREATE TABLE IF NOT EXISTS SUPPLIERS (
    supplier_id VARCHAR(15) PRIMARY KEY,
    supplier_name VARCHAR(255),
    supplier_street VARCHAR(255),
    supplier_city VARCHAR(255),
    supplier_country VARCHAR(255),
    supplier_postcode VARCHAR(255),
    supplier_contact INT,
    supplier_email VARCHAR(255)
);"

# Create table for Customer entity
sql_customers <- "
CREATE TABLE IF NOT EXISTS CUSTOMERS (
    customer_id VARCHAR(15) PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_street VARCHAR(255),
    customer_city VARCHAR(255),
    customer_country VARCHAR(255),
    customer_postcode VARCHAR(255),
    customer_email VARCHAR(255),
    customer_contact INT,
    related_id VARCHAR(15),
    FOREIGN KEY (related_id) REFERENCES CUSTOMERS(customer_id)
);"

# create table for Advertisements entity
sql_ads <- "
CREATE TABLE IF NOT EXISTS ADVERTISEMENTS (
    ads_id VARCHAR(15) PRIMARY KEY,
    ads_price INT,
    ads_startdate DATE,
    ads_enddate DATE,
    supplier_id VARCHAR(15),
    product_id VARCHAR(15),
    FOREIGN KEY (supplier_id) REFERENCES SUPPLIERS(supplier_id),
    FOREIGN KEY (product_id) REFERENCES PRODUCTS(product_id)
);"

# create table for WAREHOUSES entity
sql_warehouses <- "
CREATE TABLE IF NOT EXISTS WAREHOUSES (
    warehouse_id VARCHAR(15) PRIMARY KEY,
    warehouse_street VARCHAR(255),
    warehouse_city VARCHAR(255),
    warehouse_country VARCHAR(255),
    warehouse_postcode VARCHAR(255),
    warehouse_contact VARCHAR(255)
);"


# create table for PRODUCTS entity
sql_products <- "
CREATE TABLE IF NOT EXISTS PRODUCTS (
    product_id VARCHAR(15) PRIMARY KEY,
    product_name VARCHAR(255),
    product_reviewscore INT,
    product_price INT,
    category_id VARCHAR(15),
    FOREIGN KEY (category_id) REFERENCES CATEGORY(category_id)
);"


# create table for CATEGORY entity
sql_category <- "
CREATE TABLE IF NOT EXISTS CATEGORY (
    category_id VARCHAR(15) PRIMARY KEY,
    category_name VARCHAR(255)
);"


# create table for ORDERS_DETAILS entity
sql_order_details <- "
CREATE TABLE IF NOT EXISTS ORDERS_DETAILS (
    Order_ID VARCHAR(15) PRIMARY KEY,
    check_out_date DATE,
    order_status VARCHAR(255),
    payment_method VARCHAR(255),
    payment_status VARCHAR(255),
    payment_date DATE
);"

# Relationship Entities

# create table for SUPPLY relationship entity
sql_supply <- "
CREATE TABLE IF NOT EXISTS SUPPLY (
    product_id VARCHAR(15),
    supplier_id VARCHAR(15),
    product_cost INT,
    supply_quantity INT,
    PRIMARY KEY (product_id, supplier_id),
    FOREIGN KEY (product_id) REFERENCES PRODUCTS(product_id),
    FOREIGN KEY (supplier_id) REFERENCES SUPPLIERS(supplier_id)
);"

# create table for STORE relationship
sql_store <- "
CREATE TABLE IF NOT EXISTS STORE (
    product_id VARCHAR(15),
    warehouse_id VARCHAR(15),
    store_date DATE,
    store_quantity INT,
    PRIMARY KEY (product_id, warehouse_id, store_date),
    FOREIGN KEY (product_id) REFERENCES PRODUCTS(product_id),
    FOREIGN KEY (warehouse_id) REFERENCES WAREHOUSES(warehouse_id)
);"

# create table for ORDERS relationship
sql_orders <- "
CREATE TABLE IF NOT EXISTS ORDERS (
    order_id VARCHAR(15),
    customer_id VARCHAR(15),
    product_id VARCHAR(15),
    supplier_id VARCHAR(15),
    order_transaction_date DATE,
    order_quantity INT,
    FOREIGN KEY (order_id) REFERENCES ORDERS_DETAILS(order_id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(customer_id),
    FOREIGN KEY (product_id) REFERENCES PRODUCTS(product_id),
    FOREIGN KEY (supplier_id) REFERENCES PRODUCTS(product_id),
    PRIMARY KEY (order_id, customer_id, product_id, supplier_id)
);"


# Execute the SQL statements
result <- RSQLite::dbExecute(my_connection, sql_suppliers)
print(result)
RSQLite::dbExecute(my_connection, sql_customers)
RSQLite::dbExecute(my_connection, sql_category)
RSQLite::dbExecute(my_connection, sql_products)
RSQLite::dbExecute(my_connection, sql_ads)
RSQLite::dbExecute(my_connection, sql_warehouses)
RSQLite::dbExecute(my_connection, sql_order_details)
RSQLite::dbExecute(my_connection, sql_supply)
RSQLite::dbExecute(my_connection, sql_store)
RSQLite::dbExecute(my_connection, sql_orders)


RSQLite::dbListTables(my_connection)

# Close the database connection
RSQLite::dbDisconnect(my_connection)

