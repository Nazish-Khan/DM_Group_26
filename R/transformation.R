library(RSQLite)
library(readr)
library(stringr)
library(fs)
library(dplyr)


# Function to find the latest version of a file in a folder
find_latest_version <- function(folder_path, prefix) {
  files <- list.files(folder_path, pattern = paste0("^", prefix, "_v[0-9]+\\.csv$"))
  versions <- as.numeric(sub(paste0("^", prefix, "_v([0-9]+)\\.csv$"), "\\1", files))
  latest_version <- max(versions)
  latest_file <- paste0(prefix, "_v", latest_version, ".csv")
  return(file.path(folder_path, latest_file))
}

# Data Load and Data Integrity Check

#Adding testing comments
db_file <- "database/database.db"
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),dbname = db_file)

# Specify the folder paths for each type of data
customer_folder <- "data_upload/Customer"
supplier_folder <- "data_upload/Supplier"
advertisement_folder <- "data_upload/Advertisement"
category_folder <- "data_upload/Category"
order_details_folder <- "data_upload/Order_details"
order_folder <- "data_upload/Order"
product_folder <- "data_upload/Product"
store_folder <- "data_upload/Store"
supply_folder <- "data_upload/Supply"
warehouse_folder <- "data_upload/Warehouse"

# Load product and supplier data from the latest CSV files
customer_file <- find_latest_version(customer_folder, "customer_data")
supplier_file <- find_latest_version(supplier_folder, "supplier_data")
advertisement_file <- find_latest_version(advertisement_folder, "advertisement_data")
category_file <- find_latest_version(category_folder, "category_data")
order_details_file <- find_latest_version(order_details_folder, "order_details_data")
order_file <- find_latest_version(order_folder, "order_data")
product_file <- find_latest_version(product_folder, "product_data")
store_file <- find_latest_version(store_folder, "store_data")
supply_file <- find_latest_version(supply_folder, "supply_data")
warehouse_file <- find_latest_version(warehouse_folder, "warehouse_data")

print("Reading the data")

# Load product and supplier data from CSV files
# Reading the csv file
customer_data <- readr::read_csv(customer_file,show_col_types = FALSE)
supplier_data <- readr::read_csv(supplier_file,show_col_types = FALSE)
advertisement_data <- readr::read_csv(advertisement_file,show_col_types = FALSE)
category_data <- readr::read_csv(category_file,show_col_types = FALSE)
order_data <- readr::read_csv(order_file,show_col_types = FALSE)
order_details_data <- readr::read_csv(order_details_file,show_col_types = FALSE)
product_data <- readr::read_csv(product_file,show_col_types = FALSE)
store_data <- readr::read_csv(store_file,show_col_types = FALSE)
supply_data <- readr::read_csv(supply_file,show_col_types = FALSE)
warehouse_data <- readr::read_csv(warehouse_file,show_col_types = FALSE)


# Data Integrity Check


errors <- list()
# Check for null primary keys
null_keys <- c(
  "supplier_data" = "supplier_id",
  "customer_data" = "customer_id",
  "advertisement_data" = "ads_id",
  "warehouse_data" = "warehouse_id",
  "product_data" = "product_id",
  "category_data" = "category_id",
  "order_details_data" = "order_id"
)
# 
# Check for null primary keys
for (table_name in names(null_keys)) {
  if (any(is.na(eval(parse(text = table_name))[[null_keys[table_name]]]))) {
    errors[[table_name]] <- paste("Null primary key detected in", null_keys[table_name])
  }
}

# Check for null values in composite primary key columns
composite_keys <- list(
  "supply_data" = c("product_id", "supplier_id"),
  "store_data" = c("product_id", "warehouse_id"),
  "order_data" = c("order_id", "customer_id", "product_id", "supplier_id")
)

for (table_name1 in names(composite_keys)) {
  # Get the column names for the composite primary key
  key_columns <- composite_keys[[table_name1]]
  
  # Extract the data frame corresponding to the table name
  table_data1 <- eval(parse(text = table_name1))
  #print(table_data1)
  # Check for null values in each column of the composite primary key
  for (key_column in key_columns) {
    if (any(is.na(table_data1[[key_column]]))) {
      errors[[table_name]] <- paste("Null value detected in", key_column, "of", table_name1, "\n")
      #cat("Null value detected in", key_column, "of", table_name1, "\n")
    }
  }
}
# Function to check email format
validate_email <- function(email) {
  return(str_detect(email, "^\\S+@\\S+\\.\\S+$"))
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

# Validate Primary Key Format
# Check supplier_id format for all records
supplier_id_pattern <- "^SUP\\d{9,}$"     # Define regex pattern for supplier_id
invalid_supplier_ids <- which(!grepl(supplier_id_pattern, supplier_data$supplier_id))
if (length(invalid_supplier_ids) > 0) {
  errors$supplier_id <- paste("Invalid supplier IDs detected at index:", toString(invalid_supplier_ids))
}

# Advertisement ID Format
ads_id_pattern <- "^AD\\d{9,}$"                # Define regex pattern for ads_id
invalid_ads_ids <- which(!grepl(ads_id_pattern, advertisement_data$ads_id))
if (length(invalid_ads_ids) > 0) {
  errors$ads_id <- paste("Invalid Ads IDs detected at index:", toString(invalid_ads_ids))
}

# Customer ID Format
cust_id_pattern <- "^CUS\\d{9,}$"                # Define regex pattern for customer_id
invalid_cust_ids <- which(!grepl(cust_id_pattern, customer_data$customer_id))
if (length(invalid_cust_ids) > 0) {
  errors$customer_id <- paste("Invalid Customer IDs detected at index:", toString(invalid_cust_ids))
}

# Product ID Format
product_id_pattern <- "^P\\d{8}$"                # Define regex pattern for product_id
invalid_product_ids <- which(!grepl(product_id_pattern, product_data$product_id))
if (length(invalid_product_ids) > 0) {
  errors$product_id <- paste("Invalid Product IDs detected at index:", toString(invalid_product_ids))
}

# Category ID Format
category_id_pattern <- "^CAT\\d{10}$"                # Define regex pattern for product_id
invalid_cat_ids <- which(!grepl(category_id_pattern, category_data$category_id))
if (length(invalid_cat_ids) > 0) {
  errors$category_id <- paste("Invalid Category IDs detected at index:", toString(invalid_cat_ids))
}

# Order ID Format
ord_id_pattern <- "^ORD\\d{10}$"                # Define regex pattern for order_id
invalid_ord_ids <- which(!grepl(ord_id_pattern, order_details_data$order_id))
if (length(invalid_ord_ids) > 0) {
  errors$order_id <- paste("Invalid Order IDs detected at index:", toString(invalid_ord_ids))
}

# Warehouse ID Format
ware_id_pattern <- "^W\\d{8,}$"                # Define regex pattern for Warehouse ID
invalid_ware_ids <- which(!grepl(ware_id_pattern, warehouse_data$warehouse_id))
if (length(invalid_ware_ids) > 0) {
  errors$warehouse_id <- paste("Invalid Warehouse IDs detected at index:", toString(invalid_ware_ids))
}

# Check for duplicate records in single primary key entities

for (table_name in names(null_keys)) {
  if (anyDuplicated(eval(parse(text = table_name))[[null_keys[table_name]]])) {
    errors[[table_name]] <- paste("Duplicate records detected for", null_keys[table_name])
  }
}

# Check for duplicate records in composite primary key entities
for (table_name in names(composite_keys)) {
  key_columns <- composite_keys[[table_name]]
  table_data <- eval(parse(text = table_name))
  
  if (anyDuplicated(table_data[key_columns])) {
    errors[[table_name]] <- paste("Duplicate records detected for composite primary key in", table_name)
  }
}

# Check for errors in the errors list
if (length(errors) > 0) {
  stop("Data validation failed:", errors)
} else {
  
# Writing the data to the database
  print("Writing to the database")


#  existing_records <- RSQLite::dbReadTable(my_connection, "SUPPLIERS")
# Identify new records that don't already exist in the database
# new_records <- supplier_data[!duplicated(supplier_data$supplier_id) & !supplier_data$supplier_id %in% existing_records$supplier_id, ]
# Append only new records to the existing table
  print("Adding Supplier Data")
  RSQLite::dbWriteTable(my_connection, "SUPPLIERS", supplier_data, append = TRUE)
  print("Adding Category Data")
  RSQLite::dbWriteTable(my_connection, "CATEGORY", category_data, append = TRUE)
  print("Adding Customer Data")
  RSQLite::dbWriteTable(my_connection, "CUSTOMERS", customer_data, append = TRUE)
  print("Adding Orders Data")
  RSQLite::dbWriteTable(my_connection, "ORDERS", order_data, append = TRUE)
  print("Adding Orders Details Data")
  RSQLite::dbWriteTable(my_connection, "ORDERS_DETAILS", order_details_file, append = TRUE)
  print("Adding Products Data")
  RSQLite::dbWriteTable(my_connection, "PRODUCTS", product_data, append = TRUE)
  print("Adding Store Data")
  RSQLite::dbWriteTable(my_connection, "STORE", store_data, append = TRUE)
  print("Adding Supply Data")
  RSQLite::dbWriteTable(my_connection, "SUPPLY", supply_data, append = TRUE)
  print("Adding Warehouse Data")
  RSQLite::dbWriteTable(my_connection, "WAREHOUSES", warehouse_data, append = TRUE)
  print("Adding Ads Data")
  RSQLite::dbWriteTable(my_connection, "ADVERTISEMENTS", advertisement_data, append = TRUE)

  print("Done")
  RSQLite::dbDisconnect(my_connection)
}

