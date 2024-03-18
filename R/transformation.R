library(RSQLite)
library(readr)
library(stringr)
library(fs)
library(dplyr)
library(DBI)
library(ggplot2)


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
product_id_pattern <- "^P\\d{8,}$"                # Define regex pattern for product_id
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
  print("Adding Orders Details Data")
  RSQLite::dbWriteTable(my_connection, "ORDERS_DETAILS", order_details_file, append = TRUE)
  print("Adding Warehouse Data")
  RSQLite::dbWriteTable(my_connection, "WAREHOUSES", warehouse_data, append = TRUE)
  
  # Check referential integrity in each table where foreign keys are available

  # Get the primary key from the db
  # Execute SQL queries to fetch primary keys from each parent table
  sql_query_suppliers <- "SELECT supplier_id FROM SUPPLIERS"
  sql_query_customers <- "SELECT customer_id FROM CUSTOMERS"
  sql_query_products <- "SELECT product_id FROM PRODUCTS"
  sql_query_order_details <- "SELECT order_id FROM ORDERS_DETAILS"
  sql_query_category <- "SELECT category_id FROM CATEGORY"
  sql_query_warehouse <- "SELECT warehouse_id FROM WAREHOUSES"
  
  # Execute SQL queries and fetch results
  suppliers_db <- dbGetQuery(my_connection, sql_query_suppliers)
  customer_db <- dbGetQuery(my_connection, sql_query_customers)
  products_db <- dbGetQuery(my_connection, sql_query_products)
  order_details_db <- dbGetQuery(my_connection, sql_query_order_details)
  category_db <- dbGetQuery(my_connection, sql_query_category)  
  warehouse_db <- dbGetQuery(my_connection, sql_query_warehouse) 
  
  # Function to check referential integrity for foreign keys
  check_referential_integrity <- function(child_df, foreign_key_column, parent_df) {
    foreign_keys <- unique(child_df[[foreign_key_column]])
    parent_keys <- unique(parent_df[[foreign_key_column]])
    missing_keys <- setdiff(foreign_keys, parent_keys)
    if (length(missing_keys) > 0) {
      return(missing_keys)
    } else {
      return(NULL)
    }
  }
 
  # Function to check referential integrity for foreign keys which refer to themselves
  check_referential_integrity2 <- function(child_df, foreign_key_column, parent_df, parent_key) {
    foreign_keys <- unique(child_df[[foreign_key_column]])
    parent_keys <- unique(parent_df[[parent_key]])
    missing_keys <- setdiff(foreign_keys, parent_keys)
    if (length(missing_keys) > 0) {
      return(missing_keys)
    } else {
      return(NULL)
    }
  }
  
  
  # Product table foreign key
  missing_foreign_keys1 <- c()
  missing_foreign_keys1 <- c(missing_foreign_keys1, check_referential_integrity(product_data, "category_id", category_db))
  if (length(missing_foreign_keys1) > 0) {
    print("Missing foreign keys found:")
    print(unique(unlist(missing_foreign_keys1)))
  } else {
    print("No missing foreign keys found.")
    print("Adding Products Data")
    RSQLite::dbWriteTable(my_connection, "PRODUCTS", product_data, append = TRUE)
  }
  
  # Advertisements table foreign key
  missing_foreign_keys2 <- c()
  missing_foreign_keys2 <- c(missing_foreign_keys2, check_referential_integrity(advertisement_data, "product_id", products_db))
  missing_foreign_keys2 <- c(missing_foreign_keys2, check_referential_integrity(advertisement_data, "supplier_id", suppliers_db))
  if (length(missing_foreign_keys2) > 0) {
    print("Missing foreign keys found:")
    print(unique(unlist(missing_foreign_keys2)))
  } else {
    print("No missing foreign keys found.")
    print("Adding Ads Data")
    RSQLite::dbWriteTable(my_connection, "ADVERTISEMENTS", advertisement_data, append = TRUE)
  }  
  

  # Customer table foreign key
  missing_foreign_keys3 <- c()
  missing_foreign_keys3 <- c(missing_foreign_keys3, check_referential_integrity2(customer_data, "related_id", customer_db,"customer_id"))
  if (length(missing_foreign_keys3) > 0) {
    print("Missing foreign keys found:")
    print(unique(unlist(missing_foreign_keys3)))
  } else {
    print("No missing foreign keys found.")
    print("Adding Customer Data")
    RSQLite::dbWriteTable(my_connection, "CUSTOMERS", customer_data, append = TRUE)
  }  
  
  
  # Supply table foreign keys
  missing_foreign_keys4 <- c()
  missing_foreign_keys4 <- c(missing_foreign_keys4, check_referential_integrity(supply_data, "product_id", products_db))
  missing_foreign_keys4 <- c(missing_foreign_keys4, check_referential_integrity(supply_data, "supplier_id", suppliers_db))
  if (length(missing_foreign_keys4) > 0) {
    print("Missing foreign keys found:")
    print(unique(unlist(missing_foreign_keys4)))
  } else {
    print("No missing foreign keys found.")
    print("Adding Supply Data")
    RSQLite::dbWriteTable(my_connection, "SUPPLY", supply_data, append = TRUE)
  }  
  
  # Store table foreign keys
  missing_foreign_keys5 <- c()
  missing_foreign_keys5 <- c(missing_foreign_keys5, check_referential_integrity(store_data, "product_id", products_db))
  missing_foreign_keys5 <- c(missing_foreign_keys5, check_referential_integrity(store_data, "warehouse_id", warehouse_db))
  if (length(missing_foreign_keys5) > 0) {
    print("Missing foreign keys found:")
    print(unique(unlist(missing_foreign_keys5)))
  } else {
    print("No missing foreign keys found.")
    print("Adding Store Data")
    RSQLite::dbWriteTable(my_connection, "STORE", store_data, append = TRUE)
  } 
  
  # Orders table foreign keys
  missing_foreign_keys6 <- c()
  missing_foreign_keys6 <- c(missing_foreign_keys6, check_referential_integrity(order_data, "product_id", products_db))
  missing_foreign_keys6 <- c(missing_foreign_keys6, check_referential_integrity(order_data, "supplier_id", suppliers_db))
  missing_foreign_keys6 <- c(missing_foreign_keys6, check_referential_integrity(order_data, "order_id", order_details_db))
  missing_foreign_keys6 <- c(missing_foreign_keys6, check_referential_integrity(order_data, "customer_id", customer_db))
  if (length(missing_foreign_keys6) > 0) {
    print("Missing foreign keys found:")
    print(unique(unlist(missing_foreign_keys6)))
  } else {
    print("No missing foreign keys found.")
    print("Adding Order Data")
    RSQLite::dbWriteTable(my_connection, "ORDER", order_data, append = TRUE)
  } 

  
  # # Foreign key Referential Integrity Check
  # 
  # check_missing_foreign_keys <- function(child_df, foreign_key_column, parent_table, primary_key_column) {
  #   
  #   # Query the parent table to retrieve all primary key values
  #   query <- paste("SELECT", primary_key_column, "FROM", parent_table)
  #   parent_ids <- dbGetQuery(conn, query)[[1]]
  #   
  #   # Extract the foreign key column from the child dataframe
  #   foreign_keys <- unique(child_df[[foreign_key_column]])
  #   
  #   # Identify foreign key values not present in the parent table
  #   missing_foreign_keys <- setdiff(foreign_keys, parent_ids)
  #   
  #   # Close the database connection
  #   dbDisconnect(conn)
  #   
  #   # Return missing foreign keys
  #   return(missing_foreign_keys)
  # }
  # 
  # # Order$product_id check
  # child1_df <- order_data
  # child2_df <- supply_data
  # child3_df <- store_data
  # fk1_col <- "product_id"
  # fk2_col <- "supplier_id"
  # fk3_col <- "customer_id"
  # fk4_col <- "order_id"
  # parent1 <- "PRODUCTS"
  # parent2 <- "SUPPLIERS"
  # parent3 <- "CUSTOMERS"
  # parent4 <- "ORDER_DETAILS"
  # 
  # 
  # missing_keys_order1 <- check_missing_foreign_keys(child1_df, fk1_col, parent1, fk1_col)
  # missing_keys_order2 <- check_missing_foreign_keys(child1_df, fk2_col, parent2, fk2_col)
  # missing_keys_order3 <- check_missing_foreign_keys(child1_df, fk3_col, parent3, fk3_col)
  # missing_keys_order4 <- check_missing_foreign_keys(child1_df, fk4_col, parent4, fk4_col)
  # print(missing_keys)
  # 
  # 
  # if (length(missing_keys_order1) > 0 & length(missing_keys_order2) > 0 & length(missing_keys_order3) > 0 & length(missing_keys_order4) > 0) {
  #   error_msg <- paste("Missing foreign keys found in", parent_table, "for column", foreign_key_column, ":", missing_keys)
  #   print("Order foreign key missing error")
  #   stop(error_msg)
  # } else{
  #   print("Adding Orders Data")
  #   RSQLite::dbWriteTable(my_connection, "ORDERS", order_data, append = TRUE)
  # }


  print("Writing Table is done")
  
  RSQLite::dbDisconnect(my_connection)
}

# # Connect to database
# con <- dbConnect(RSQLite::SQLite(), "database/database.db")
# 
# 
# # Define function to check foreign keys
# check_foreign_key <- function(table_name, foreign_key, reference_table) {
#   query <- paste0("SELECT DISTINCT ", foreign_key, " FROM ", table_name, " WHERE ", foreign_key, " NOT IN (SELECT DISTINCT ", foreign_key, " FROM ", reference_table, ")")
#   result <- dbGetQuery(con, query)
#   return(result)
# }
# 
# # Check foreign keys for each column
# product_id_check <- check_foreign_key("ORDERS", "product_id", "PRODUCTS")
# customer_id_check <- check_foreign_key("ORDERS", "customer_id", "CUSTOMERS")
# supplier_id_check <- check_foreign_key("ORDERS", "supplier_id", "SUPPLIERS")
# order_id_check <- check_foreign_key("ORDERS", "order_id", "ORDERS_DETAILS")
# 
# # Print or inspect the results
# print(product_id_check)
# print(customer_id_check)
# print(supplier_id_check)
# print(order_id_check)
# 
# # Disconnect from database
# dbDisconnect(con)

# Data Visualization

conn <- RSQLite::dbConnect(RSQLite::SQLite(),dbname = "database/database.db")
# Get data from database into a dataframe
tables_to_read <- c("ADVERTISEMENTS", "WAREHOUSES", "SUPPLY","STORE","PRODUCTS","ORDERS_DETAILS","ORDERS","CUSTOMERS","SUPPLIERS","CATEGORY")

# Initialize an empty list to store data frames
dfs <- list()

# Loop through each table
for (table in tables_to_read) {
  # Read data from the table into a data frame
  query <- paste("SELECT * FROM", table)
  dfs[[table]] <- dbGetQuery(conn, query)
}

product <- dfs$PRODUCTS
order <- dfs$ORDERS
category <- dfs$CATEGORY
RSQLite::dbDisconnect(conn)


# 2) Average review scores (total customer value) for different product categories.
# Assuming 'products' contains 'product_reviewscore' and 'category_id'
# Calculate average review scores for each category
category_review_scores <- product %>%
  group_by(category_id) %>%
  summarise(average_review_score = mean(product_reviewscore, na.rm = TRUE))

# Join with 'CATEGORY' to get category names
category_review_scores <- category_review_scores %>%
  left_join(category, by = "category_id")

# Bar plot of average review scores by category
plot <- ggplot(category_review_scores, aes(x = reorder(category_name, -average_review_score), y = average_review_score)) +
  geom_bar(stat = "identity", fill = "gold") +
  geom_text(aes(label = round(average_review_score, 2)), vjust = -0.5, color = "dimgrey", size = 3.5) + 
  labs(title = "Average Customer Values by Category",
       x = "Category testing again again",
       y = "Average Review Score") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  ylim(0, 5)

plot

# Define the directory to save the figures
figure_directory <- "figures/"

# Create the directory if it doesn't exist
if (!dir.exists(figure_directory)) {
  dir.create(figure_directory)
}

# Save each plot as an image
this_filename_date <- as.character(Sys.Date())
this_filename_time <- as.character(format(Sys.time(), format = "%H_%M"))

# Save top_10 plot
ggsave(filename = paste0(figure_directory, "plot_", this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = plot)


# Save plot as a picture file (e.g., PNG)
# plot_filename <- "figures/plot.png"
# png(file = plot_filename, width = 800, height = 600)
# print(plot)
# dev.off()

# # Move the saved picture to the figures folder
# destination_folder <- "figures"
# file.rename(plot_filename, file.path(destination_folder, plot_filename))

