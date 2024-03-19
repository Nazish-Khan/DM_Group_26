# Load the necessary libraries

library(RSQLite)
library(readr)
library(stringr)
library(dplyr)
library(ggplot2)
library(patchwork)


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

  print("Writing Table is done")
  RSQLite::dbDisconnect(my_connection)
}


# Part 4: Data Analysis and Visualization


# Connection to the database
conn <- RSQLite::dbConnect(RSQLite::SQLite(),dbname = "database/database.db")
# Get data from database into a dataframe
tables_to_read <- c("ADVERTISEMENTS", "WAREHOUSES", "SUPPLY","STORE","PRODUCTS"
                    ,"ORDERS_DETAILS","ORDERS","CUSTOMERS","SUPPLIERS","CATEGORY")

# Initialize an empty list to store data frames
dfs <- list()

# Loop through each table
for (table in tables_to_read) {
  # Read data from the table into a data frame
  query <- paste("SELECT * FROM", table)
  dfs[[table]] <- dbGetQuery(conn, query)
}

product <- dfs$PRODUCTS
order_details <- dfs$ORDERS_DETAILS
category <- dfs$CATEGORY
order <- dfs$ORDERS
customer <- dfs$CUSTOMERS
supplier <- dfs$SUPPLIERS
warehouse <- dfs$WAREHOUSES
supply <- dfs$SUPPLY
store <- dfs$STORE
ads <- dfs$ADVERTISEMENTS

# Write the SQL query to join the tables
sql_query <- "
SELECT *
FROM `ORDERS` o
JOIN PRODUCTS p ON o.product_id = p.product_id
JOIN SUPPLIERS s ON o.supplier_id = s.supplier_id
JOIN ORDERS_DETAILS od ON o.order_id = od.order_id
JOIN CUSTOMERS c ON o.customer_id = c.customer_id
"
sql_supply_query <- "
SELECT *
FROM `SUPPLY` s
JOIN PRODUCTS p ON s.product_id = p.product_id
"

# Execute the SQL query and fetch the result into a dataframe
result_df <- dbGetQuery(conn, sql_query)

supply_product_df <- dbGetQuery(conn, sql_supply_query)


RSQLite::dbDisconnect(conn)

# Define the color palette
color_palette <- c("purple","pink","darkblue","blue",
                   "lightblue","black","darkgrey","white")

# Check for duplicate column names
duplicate_columns <- anyDuplicated(names(result_df)) > 0

# If there are duplicate column names, remove them
if (duplicate_columns) {
  result_df <- result_df[, !duplicated(names(result_df))]
}

# Define the directory to save the figures
figure_directory <- "figures/"

# Create the directory if it doesn't exist
if (!dir.exists(figure_directory)) {
  dir.create(figure_directory)
}

# Save each plot as an image
this_filename_date <- as.character(Sys.Date())
this_filename_time <- as.character(format(Sys.time(), format = "%H_%M"))

# 4.1 - Product Performance Analysis

# Top-Bottom Selling Products:

# Best-selling products / low-selling products and their categories.

# Summarize total quantity sold by product
product_sales_grouped <- result_df %>%
  group_by(product_name, category_id) %>%
  summarise(total_quantity_sold = sum(order_quantity), .groups = 'drop')

# Find the top 5 best-selling products
top_products <- product_sales_grouped %>%
  arrange(desc(total_quantity_sold)) %>%
  slice_head(n = 5)

# Find the below 5 low-selling products
low_products <- product_sales_grouped %>%
  arrange(total_quantity_sold) %>%
  slice_head(n = 5)

# Bar plot of best-selling products
top10_product <- ggplot(top_products, aes(x = total_quantity_sold, 
                                          y = reorder(product_name, total_quantity_sold), fill = category_id)) +
  geom_bar(stat = "identity") +
  labs(title = "Top 5 Best-Selling Products",
       x = "Total Quantity Sold",
       y = "Product Name") +
  theme_minimal() +
  scale_fill_manual(values = color_palette) +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1),
        panel.background = element_rect(fill = "white"),
        legend.position = "none")

# Bar plot of low-selling products
bottom10_product <- ggplot(low_products, aes(x = total_quantity_sold, 
                                             y = reorder(product_name, desc(total_quantity_sold)), fill = category_id)) +
  geom_bar(stat = "identity") +
  labs(title = "Below 5 Low-Selling Products",
       x = "Total Quantity Sold",
       y = "Product Name") +
  theme_minimal() +
  scale_fill_manual(values = color_palette) +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1),
        panel.background = element_rect(fill = "white"),
        legend.position = "none")

# Arrange plots into a single plot
combined_plot <- top10_product + bottom10_product +
  plot_layout(nrow = 2)

# Print the combined plot
print(combined_plot)

#Reviews based Order Status Change Analysis:

# Density plot
density_plot <- ggplot(result_df, aes(x = product_reviewscore, fill = order_status)) +
  geom_density(alpha = 0.8) +
  labs(title = "Density of Product Review Scores by Order Status",
       x = "Product Review Score",
       y = "Density",
       fill = "Order Status") +
  scale_fill_manual(values = color_palette) +
  theme_minimal()


# Save plot figure
ggsave(filename = paste0(figure_directory, "selling_product_trend_", 
                         this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = combined_plot)
ggsave(filename = paste0(figure_directory, "rating_status_plot_", 
                         this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = density_plot)

# Print the plot
print(density_plot)

#4.2 - Product-Category Analysis

#Reviews based Category Analysis:


# Calculate average review scores for each category
category_review_scores <- product %>%
  group_by(category_id) %>%
  summarise(average_review_score = mean(product_reviewscore, na.rm = TRUE))

# Join with 'CATEGORY' to get category names
category_review_scores <- category_review_scores %>%
  left_join(category, by = "category_id")

# Bar plot of average review scores by category
avg_review_plot <- ggplot(category_review_scores, 
                          aes(x = reorder(category_name, -average_review_score), y = average_review_score, fill = category_id)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = round(average_review_score, 2)), vjust = -0.5, 
            color = "black", size = 3.5) + 
  labs(title = "Average Customer Values by Category",
       x = "Category",
       y = "Average Review Score") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_fill_manual(values = color_palette) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1), 
        panel.background = element_rect(fill = "white", color = "blue"),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line = element_line(color = "blue"),
        axis.title = element_text(color = "blue"),
        axis.text = element_text(color = "blue"),
        legend.position = "none")

print(avg_review_plot)

#Profit Margin Analysis:

# Calculate profit margin
supply_product_df_origin <- supply_product_df # copy for a return data
supply_product_df$profit_margin <- supply_product_df$product_price - 
  supply_product_df$product_cost

# Check for duplicate column names
duplicate_columns1 <- anyDuplicated(names(supply_product_df)) > 0

# If there are duplicate column names, remove them
if (duplicate_columns1) {
  supply_product_df <- supply_product_df[, !duplicated(names(supply_product_df))]
}

# Mutate the Category ID to name
matchindex <- match(supply_product_df$category_id,category_data$category_id)
supply_product_df$category_id <- category_data$category_name[matchindex]

# Margin Plot for the product wise profit average
margin_plot <- ggplot(supply_product_df, aes(x = category_id, y = profit_margin)) +
  geom_boxplot() +  # Boxplot to show distribution
  stat_summary(fun.y = mean, geom = "line", aes(group = 1), color = "purple") +
  stat_summary(fun.y = min, geom = "line", aes(group = 1), color = "grey") + 
  stat_summary(fun.y = max, geom = "line", aes(group = 1), color = "blue") + 
  labs(title = "Profit Margin by Product",
       x = "Product Name",
       y = "Profit Margin") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))+ # Rotate x-axis labels
  scale_fill_manual(values = color_palette) +
  theme_minimal()


# Save plot figure
ggsave(filename = paste0(figure_directory, "review_category_plot_", 
                         this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = avg_review_plot)
ggsave(filename = paste0(figure_directory, "profit_margin_plot_", 
                         this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = margin_plot)

## return the data to its original form again ##
supply_product_df <- supply_product_df_origin

print(margin_plot)

# 4.3 - Customer Analysis

#Customer Segmentation:

# Segment customers based on country
customers_country <- customer %>%
  group_by(customer_country) %>%
  summarise(total_customers = n())

# Plot for number of customers by country
cust_segm <- ggplot(customers_country, aes(x = customer_country, y = total_customers, 
                                           fill = customer_country)) +
  geom_bar(stat = "identity") +
  labs(title = "Number of Customers by Country",
       x = "Country",
       y = "Number of Customers") +
  #scale_fill_discrete(name = "Country")+
  scale_fill_manual(values = color_palette) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1), 
        panel.background = element_rect(fill = "white", color = "blue"),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line = element_line(color = "blue"),
        axis.title = element_text(color = "blue"),
        axis.text = element_text(color = "blue"),
        legend.title = element_text(color = "blue"),
        legend.text = element_text(color = "blue"))

print(cust_segm)


#Customer Preferences in France: 


# Merge category name to the result_df table
matchindex <- match(result_df$category_id,category$category_id)
result_df$category_name <- category$category_name[matchindex]

# Select France customers
product_fra <- result_df %>%
  filter(customer_country == "France")
product_fra_cate <- product_fra %>%
  group_by(category_name) %>%
  summarise(quantity=sum(order_quantity))

# Calculate the percentage of product category in France
category_totals <- aggregate(quantity ~ category_name, data = product_fra_cate, sum)
total_quantity <- sum(category_totals$quantity)
category_totals$percentage <- category_totals$quantity / total_quantity * 100

# Plot the pie chart
cust_france_plot <- ggplot(category_totals, aes(x = "", y = percentage, fill = category_name)) +
  geom_bar(stat = "identity", width = 1) +
  coord_polar("y", start = 0) +
  geom_text(aes(label = paste(round(percentage, 1), "%")), color = 'white',position = position_stack(vjust = 0.5)) +
  scale_fill_manual(values = color_palette) +
  labs(title = "Customer Preferences in France",
       fill = "Category",
       x = NULL, y = NULL) +
  theme_void() +
  theme(legend.position = "right") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1), 
        panel.background = element_rect(fill = "white", color = "blue"),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line = element_line(color = "blue"),
        axis.title = element_text(color = "blue"),
        axis.text = element_text(color = "blue"),
        legend.title = element_text(color = "blue"),
        legend.text = element_text(color = "blue"))


# Save plot figure
ggsave(filename = paste0(figure_directory, "customer_prefrences_plot-", 
                         this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = cust_france_plot)
print(cust_france_plot)


#Payment Method Trend:


# Create the payment analysis ggplot
payment_trend_plot <- ggplot(result_df, aes(x = payment_date, fill = payment_method)) +
  geom_bar(stat = "count", position = "dodge") +
  #scale_x_date(date_labels = "%b %Y", date_breaks = "1 month") +
  labs(title = "Payment Method Usage Over Time",
       x = "Payment Date",
       y = "Number of Payments",
       fill = "Payment Method") +
  scale_fill_manual(values = color_palette) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1), 
        panel.background = element_rect(fill = "white", color = "blue"),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line = element_line(color = "blue"),
        axis.title = element_text(color = "blue"),
        axis.text = element_text(color = "blue"),
        legend.title = element_text(color = "blue"),
        legend.text = element_text(color = "blue"))




# Save plot figure
ggsave(filename = paste0(figure_directory, "customer_segment_", 
                         this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = cust_segm)
ggsave(filename = paste0(figure_directory, "payment_trend_plot_", 
                         this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = payment_trend_plot)

# Print the plot
print(payment_trend_plot)


#4.4 - Ads vs Supplier Analysis

#Top-Bottom Supplier Contribution in Ads Revenue:

# Advertisement Analysis

# Convert date format to Date type
ads$ads_startdate <- as.Date(ads$ads_startdate, format = "%d/%m/%y")
ads$ads_enddate <- as.Date(ads$ads_enddate, format = "%d/%m/%y")

# Calculate duration in days
ads$duration <- as.numeric(difftime(ads$ads_enddate, ads$ads_startdate, units = "days"))


ads$price_per_day <- round(ads$ads_price / ads$duration,2)

# Find the top 5 suppliers paying the highest price per day
top_suppliers <- ads %>%
  arrange(desc(price_per_day)) %>%
  slice_head(n = 5) %>%
  group_by(supplier_id) %>%
  summarise(avg_price_per_day = mean(price_per_day), .groups = 'drop')

# Find the bottom 5 suppliers paying the lowest price per day
bottom_suppliers <- ads %>%
  arrange(price_per_day) %>%
  slice_head(n = 5) %>%
  group_by(supplier_id) %>%
  summarise(avg_price_per_day = mean(price_per_day), .groups = 'drop')


# Bar plot of top 5 suppliers paying the highest price per day
top5_suppliers_plot <- ggplot(top_suppliers, aes(x = avg_price_per_day, 
                                                 y = reorder(supplier_id, avg_price_per_day), fill = supplier_id)) +
  geom_bar(stat = "identity") +
  labs(title = "Top 5 Suppliers with Highest Price per Day",
       x = "Average Price per Day",
       y = "Supplier ID") +
  scale_fill_manual(values = color_palette) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1),
        panel.background = element_rect(fill = "white"),
        legend.position = "none")

# Bar plot of bottom 5 suppliers paying the lowest price per day
bottom5_suppliers_plot <- ggplot(bottom_suppliers, aes(x = avg_price_per_day, 
                                                       y = reorder(supplier_id, avg_price_per_day), fill = supplier_id)) +
  geom_bar(stat = "identity") +
  labs(title = "Bottom 5 Suppliers with Lowest Price per Day",
       x = "Average Price per Day",
       y = "Supplier ID") +
  theme_minimal() +
  scale_fill_manual(values = color_palette) +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1),
        panel.background = element_rect(fill = "white"),
        legend.position = "none")

# Arrange plots into a single plot
combined_suppliers_plot <- top5_suppliers_plot + bottom5_suppliers_plot +
  plot_layout(nrow = 2)


# Save plot figure

ggsave(filename = paste0(figure_directory, "ads_supplier_plot_", 
                         this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = combined_suppliers_plot)

# Print the combined plot
print(combined_suppliers_plot)