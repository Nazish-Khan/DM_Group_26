# Function to perform data integrity, email format, and duplicate checks
process_data <- function(customer_data, supplier_data, advertisement_data, category_data, order_payment_data,order_details_data, order_data, product_data, store_data, supply_data, warehouse_data) {
errors <- list()

# Check data integrity for primary key being null
null_keys <- list(
  "customer_data" = "customer_id",
  "supplier_data" = "supplier_id",
  "advertisement_data" = "ads_id",
  "category_data" = "category_id",
  "order_details_data" = "order_id",
  "warehouse_data" = "warehouse_id",
  "product_data" = "product_id"
)

# Check for null primary keys
for (table_name in names(null_keys)) {
  if (any(is.na(eval(parse(text = table_name))[[null_keys[table_name]]]))) {
    errors[[table_name]] <- paste("Null primary key detected in", null_keys[table_name])
  }
}

# Check for composite keys and null values
composite_keys <- list(
  "supply_data" = c("product_id", "suppliers_id"),
  "store_data" = c("product_id", "warehouse_id"),
  "order_data" = c("order_details_id", "customer_id", "product_id", "supplier_id")
)

for (table_name in names(composite_keys)) {
  key_columns <- composite_keys[[table_name]]
  table_data <- get(table_name)
  
  for (key_column in key_columns) {
    if (any(is.na(table_data[[key_column]]))) {
      error_msg <- paste("Null value detected in", key_column, "of", table_name)
      if (exists(errors[[table_name]])) {
        errors[[table_name]] <- paste(errors[[table_name]], error_msg)
      } else {
        errors[[table_name]] <- error_msg
      }
    }
  }
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
duplicated_records <- list(
  "customer_data" = "customer_id",
  "supplier_data" = "supplier_id",
  "advertisement_data" = "ads_id",
  "category_data" = "category_id",
  "order_details_data" = "order_id",
  "warehouse_data" = "warehouse_id",
  "product_data" = "product_id"
)

for (table_name in names(duplicated_records)) {
  if (anyDuplicated(get(table_name)[[duplicated_records[table_name]]])) {
    errors[[table_name]] <- paste("Duplicate records detected for", duplicated_records[table_name])
  }
}

# Check for duplicate records in composite key entities
duplicated_records_composite <- list(
  "supply_data" = c("product_id", "suppliers_id"),
  "store_data" = c("product_id", "warehouse_id"),
  "order_data" = c("order_details_id", "customer_id", "product_id", "supplier_id")
)

for (table_name in names(duplicated_records_composite)) {
  key_columns <- duplicated_records_composite[[table_name]]
  data <- get(table_name)
  duplicate_indices <- duplicated(data[, key_columns]) | duplicated(data[, key_columns], fromLast = TRUE)
  if (any(duplicate_indices)) {
    errors[[table_name]] <- paste("Duplicate records detected in", table_name, "at row(s):", toString(which(duplicate_indices)))
  }
}

# If there are errors, stop the process and throw an error
if (length(errors) > 0) {
  stop("Data validation failed:", errors)
}

  return(errors)
}


processed_data <- process_data(customer_data, supplier_data, advertisement_data, category_data, order_payment_data,
                               order_details_data, order_data, product_data, store_data, supply_data, warehouse_data)


