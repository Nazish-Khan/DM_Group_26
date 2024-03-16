library(RSQLite)
library(readr)
library(stringr)

# Data Load and Data Integrity Check

#Adding testing comments
db_file <- "database/database.db"
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),dbname = db_file)

print("Reading the data")

# Load product and supplier data from CSV files
# Reading the csv file
customer_data <- readr::read_csv("data_upload/Customer_data_with_related_id.csv",show_col_types = FALSE)
supplier_data <- readr::read_csv("data_upload/supplier_data.csv",show_col_types = FALSE)
advertisement_data <- readr::read_csv("data_upload/Advertisements_data.csv",show_col_types = FALSE)
category_data <- readr::read_csv("data_upload/Category_data.csv",show_col_types = FALSE)
order_data <- readr::read_csv("data_upload/Order_data.csv",show_col_types = FALSE)
order_details_data <- readr::read_csv("data_upload/Order_details_data.csv",show_col_types = FALSE)
#order_payment_data <- readr::read_csv("data_upload/Order_payment_data.csv",show_col_types = FALSE)
product_data <- readr::read_csv("data_upload/Product_data.csv",show_col_types = FALSE)
store_data <- readr::read_csv("data_upload/Store_data.csv",show_col_types = FALSE)
supply_data <- readr::read_csv("data_upload/Supply_data.csv",show_col_types = FALSE)
warehouse_data <- readr::read_csv("data_upload/Warehouse_data.csv",show_col_types = FALSE)

# Function to check email format
validate_email <- function(email) {
  return(str_detect(email, "^\\S+@\\S+\\.\\S+$"))
}

# Function to perform data integrity, email format, and duplicate checks
process_data <- function(customer_data, supplier_data, advertisement_data,category_data,order_payment_data,order_details_data,order_data,product_data,store_data,supply_data,warehouse_data) {
  errors <- list()
  
  # Check data integrity for primary key being null
  if (anyNA(customer_data$Customer_id)) {
    errors$Customer_id <- "Customer ID cannot be NULL."
  }
  if (anyNA(supplier_data$Supplier_id)) {
    errors$Supplier_id <- "Supplier ID cannot be NULL."
  }
  if (anyNA(advertisement_data$Ads_id)) {
    errors$Ads_id <- "Ads ID cannot be NULL."
  }
  if (anyNA(category_data$Category_id)) {
    errors$Category_id <- "Category ID cannot be NULL."
  }
  if (anyNA(order_details_data$Order_id)) {
    errors$Order_id <- "Order ID cannot be NULL."
  }
  if (anyNA(warehouse_data$Warehouse_id)) {
    errors$Warehouse_id <- "Warehouse ID cannot be NULL."
  }
  if (anyNA(advertisement_data$Ads_id)) {
    errors$Ads_id <- "Ads ID cannot be NULL."
  }
  if (anyNA(category_data$Category_id)) {
    errors$Category_id <- "Category ID cannot be NULL."
  }
  if (anyNA(product_data$Product_id)) {
    errors$Product_id <- "Product ID cannot be NULL."
  }
  
  # Ask for referential integrity question
  
  # Validate email format
  invalid_customer_emails <- which(!validate_email(customer_data$customer_email))
  if (length(invalid_customer_emails) > 0) {
    errors$customer_email <- paste("Invalid email format detected for customers:", toString(invalid_customer_emails))
  }
  invalid_supplier_emails <- which(!validate_email(supplier_data$supplier_email))
  if (length(invalid_supplier_emails) > 0) {
    errors$supplier_email <- paste("Invalid email format detected for suppliers:", toString(invalid_supplier_emails))
  }
  
  # I will create function for this
  # Check for duplicate records
  duplicated_customers <- duplicated(customer_data$Customer_id)
  duplicated_suppliers <- duplicated(supplier_data$Supplier_id)
  duplicated_advertisements <- duplicated(advertisement_data$Ads_id)
  duplicated_category<- duplicated(category_data$Category_id)
  duplicated_order_details<- duplicated(order_details_data$Order_id)
  duplicated_warehouse<- duplicated(warehouse_data$Warehouse_id)
  duplicated_product<- duplicated(product_data$Product_id)
  
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

# Add if condition for the check and don't run if there is any error


# Writing the data to the database
print("Writing to the database")
#RSQLite::dbRemoveTable(my_connection,"CUSTOMERS")
RSQLite::dbWriteTable(my_connection,"CUSTOMERS",customer_data, append = TRUE)
# Retrieve existing records from the database table
existing_records <- RSQLite::dbReadTable(my_connection, "SUPPLIERS")

# Identify new records that don't already exist in the database
new_records <- supplier_data[!duplicated(supplier_data$Supplier_id) & !supplier_data$Supplier_id %in% existing_records$Supplier_id, ]

# Append only new records to the existing table
RSQLite::dbWriteTable(my_connection, "SUPPLIERS", new_records, append = TRUE)

#RSQLite::dbRemoveTable(my_connection,"SUPPLIERS")
#RSQLite::dbWriteTable(my_connection,"SUPPLIERS",supplier_data, append = TRUE)

print("Done")
RSQLite::dbDisconnect(my_connection)

db_file <- "database/database.db"
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

