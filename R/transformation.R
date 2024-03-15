library(RSQLite)
library(readr)
library(stringr)

# Create the Tables
# Specify the database file name
db_file <- "database.db"

# Create the db connection with SQLite
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),dbname = db_file)
print(my_connection)

# Delete if tables already exist
#drop_tables <- "DROP TABLE IF EXISTS SUPPLIERS, CUSTOMERS, PRODUCTS, ADS, WAREHOUSE, CATEGORY, ORDER_DETAILS, ORDERS_PAYMENT, SUPPLY, STORE, ORDERS;"

# Entity Table Creation

# Create table for SUPPLIERS entity
sql_suppliers <- "
CREATE TABLE IF NOT EXISTS SUPPLIERS (
    Supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(255),
    supplier_street VARCHAR(255),
    supplier_city VARCHAR(255),
    supplier_country VARCHAR(255),
    supplier_postcode VARCHAR(255),
    supplier_phoneno INT,
    supplier_email VARCHAR(255),
    category_id VARCHAR(255),
    FOREIGN KEY (category_id) REFERENCES CATEGORY(category_id)
);"

# Create table for Customer entity
sql_customers <- "
CREATE TABLE IF NOT EXISTS CUSTOMERS (
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(255),
    Customer_Address VARCHAR(255),
    Customer_Email VARCHAR(255),
    Customer_no INT,
    Related_ID INT,
    FOREIGN KEY (Related_ID) REFERENCES CUSTOMERS(Customer_ID)
);"

# create table for Advertisements entity
sql_ads <- "
CREATE TABLE IF NOT EXISTS ADS (
    Ads_ID INT PRIMARY KEY,
    Ads_Price INT,
    Ad_Description VARCHAR(255),
    Ad_Duration FLOAT,
    Supplier_id INT,
    Product_ID INT,
    FOREIGN KEY (Supplier_id) REFERENCES SUPPLIERS(Supplier_id),
    FOREIGN KEY (Product_ID) REFERENCES PRODUCTS(Product_ID)
);"

# create table for WAREHOUSE entity
sql_warehouse <- "
CREATE TABLE IF NOT EXISTS WAREHOUSE (
    Warehouse_ID INT PRIMARY KEY,
    Warehouse_Address VARCHAR(255),
    Warehouse_Contact VARCHAR(255)
);"


# create table for PRODUCTS entity
sql_products <- "
CREATE TABLE IF NOT EXISTS PRODUCTS (
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(255),
    Product_review INT,
    Category_ID INT,
    FOREIGN KEY (Category_ID) REFERENCES CATEGORY(Category_ID)
);"


# create table for CATEGORY entity
sql_category <- "
CREATE TABLE IF NOT EXISTS CATEGORY (
    Category_ID INT PRIMARY KEY,
    Category_Description VARCHAR(255),
    Category_name VARCHAR(255)
);"


# create table for ORDERS_DETAILS entity
sql_order_details <- "
CREATE TABLE IF NOT EXISTS ORDERS_DETAILS (
    Order_ID INT PRIMARY KEY,
    Check_out_date DATE,
    Order_status VARCHAR(255)
);"


# create table for ORDERS_PAYMENT entity
sql_orders_payment <- "
CREATE TABLE IF NOT EXISTS ORDERS_PAYMENT (
    Order_ID INT PRIMARY KEY,
    Payment_method VARCHAR(255),
    Payment_status VARCHAR(255),
    Payment_date DATE
);"

# Relationship Entities

# create table for SUPPLY relationship entity
sql_supply <- "
CREATE TABLE IF NOT EXISTS SUPPLY (
    Product_ID INT,
    Supplier_ID INT,
    Supply_cost INT,
    Supply_quantity INT,
    Supply_margin INT,
    PRIMARY KEY (Product_ID, Supplier_ID),
    FOREIGN KEY (Product_ID) REFERENCES PRODUCTS(Product_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES SUPPLIERS(Supplier_ID)
);"

# create table for STORE relationship
sql_store <- "
CREATE TABLE IF NOT EXISTS STORE (
    Product_ID INT,
    Warehouse_ID INT,
    Store_Quantity INT,
    Store_Date DATE,
    PRIMARY KEY (Product_ID, Warehouse_ID),
    FOREIGN KEY (Product_ID) REFERENCES PRODUCTS(Product_ID),
    FOREIGN KEY (Warehouse_ID) REFERENCES WAREHOUSE(Warehouse_ID)
);"

# create table for ORDERS relationship
sql_orders <- "
CREATE TABLE IF NOT EXISTS ORDERS (
    Order_details_ID INT PRIMARY KEY,
    Customer_ID INT,
    Product_ID INT,
    Order_price INT,
    Order_transaction_date DATE,
    Order_discount INT,
    Order_quantity INT,
    FOREIGN KEY (Order_details_ID) REFERENCES ORDERS_DETAILS(Order_details_ID),
    FOREIGN KEY (Customer_ID) REFERENCES CUSTOMERS(Customer_ID),
    FOREIGN KEY (Product_ID) REFERENCES PRODUCTS(Product_ID)
);"


# Execute the SQL statements
result <- RSQLite::dbExecute(my_connection, sql_suppliers)
print(result)
RSQLite::dbExecute(my_connection, sql_customers)
RSQLite::dbExecute(my_connection, sql_category)
RSQLite::dbExecute(my_connection, sql_products)
RSQLite::dbExecute(my_connection, sql_ads)
RSQLite::dbExecute(my_connection, sql_warehouse)
RSQLite::dbExecute(my_connection, sql_order_details)
RSQLite::dbExecute(my_connection, sql_orders_payment)
RSQLite::dbExecute(my_connection, sql_supply)
RSQLite::dbExecute(my_connection, sql_store)
RSQLite::dbExecute(my_connection, sql_orders)

# Close the database connection
#dbDisconnect(my_connection)

RSQLite::dbListTables(my_connection)
RSQLite::dbDisconnect(my_connection)


# Check the Schema of the database after creating the tables



# Specify the database file name
db_file <- "database.db"

# Establish a connection to the SQLite database
my_connection <- dbConnect(RSQLite::SQLite(), dbname = db_file)

# List all tables in the database
tables <- dbListTables(my_connection)

# Iterate over tables and print the schema
for (table in tables) {
  cat("Table:", table, "\n")
  fields <- dbListFields(my_connection, table)
  print(fields)
  cat("\n")
}

# Close the database connection
dbDisconnect(my_connection)

# Data Load and Data Integrity Check

#Adding testing comments
db_file <- "database.db"
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),dbname = db_file)

print("Reading the data")

# Load product and supplier data from CSV files
# Reading the csv file
customer_data <- readr::read_csv("data_upload/Customer_data_with_related_id.csv",show_col_types = FALSE)
supplier_data <- readr::read_csv("data_upload/supplier_data.csv",show_col_types = FALSE)


# Function to check email format
validate_email <- function(email) {
  return(str_detect(email, "^\\S+@\\S+\\.\\S+$"))
}

# Function to perform data integrity, email format, and duplicate checks
process_data <- function(product_df, supplier_data) {
  errors <- list()
  
  # Check data integrity
  if (anyNA(customer_data$Customer_id)) {
    errors$Customer_id <- "Customer ID cannot be NULL."
  }
  if (anyNA(supplier_data$Supplier_id)) {
    errors$Supplier_id <- "Supplier ID cannot be NULL."
  }
  
  # Validate email format
  invalid_customer_emails <- which(!validate_email(customer_data$customer_email))
  if (length(invalid_customer_emails) > 0) {
    errors$customer_email <- paste("Invalid email format detected for customers:", toString(invalid_customer_emails))
  }
  invalid_supplier_emails <- which(!validate_email(supplier_data$supplier_email))
  if (length(invalid_supplier_emails) > 0) {
    errors$supplier_email <- paste("Invalid email format detected for suppliers:", toString(invalid_supplier_emails))
  }
  
  # Check for duplicate records
  duplicated_customers <- duplicated(customer_data$Customer_id)
  duplicated_suppliers <- duplicated(supplier_data$Supplier_id)
  #print(duplicated_suppliers)
  if (any(duplicated_customers)) {
    errors$Customer_id <- paste("Duplicate customer IDs detected at record - :", toString(which(duplicated_customers)))
  }
  if (any(duplicated_suppliers)) {
    errors$Supplier_id <- paste("Duplicate supplier IDs detected at record - :", toString(which(duplicated_suppliers)))
  }
  
  # If there are errors, stop the process and throw an error
  if (length(errors) > 0) {
    stop("Data validation failed:", errors)
  }
  return(errors)
}


# Process data and perform checks
processed_data <- process_data(customer_data, supplier_data)
#

#Add if condition for the check and don't run if there is any error
# # Save the processed data to the database
# dbWriteTable(con, "processed_data", processed_data, overwrite=TRUE)

# Close the database connection
#dbDisconnect(con)
# Writing the data to the database
print("Writing to the database")
RSQLite::dbRemoveTable(my_connection,"CUSTOMERS")
RSQLite::dbWriteTable(my_connection,"CUSTOMERS",customer_data)

RSQLite::dbRemoveTable(my_connection,"SUPPLIERS")
RSQLite::dbWriteTable(my_connection,"SUPPLIERS",supplier_data)

print("Done")
RSQLite::dbDisconnect(my_connection)

db_file <- "database.db"
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),dbname = db_file)

# SQL query to select product_id and supplier_id based on category match
sql_query1 <- "
            SELECT *
            FROM CUSTOMERS;
            "
sql_query2 <- "
            SELECT *
            FROM SUPPLIERS;
            "
# Execute the SQL query and fetch the result
result <- RSQLite::dbGetQuery(my_connection, sql_query1)
result2 <- RSQLite::dbGetQuery(my_connection, sql_query2)

# Close the database connection
RSQLite::dbDisconnect(my_connection)

# Display the result
print(result)
print(result2)

