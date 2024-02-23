library(RSQLite)
library(readr)
print("Reading the data")
# Reading the csv file
product_data <- readr::read_csv("data_upload/MOCK_DATA_PRODUCTS.csv",show_col_types = FALSE)

# Writing the data to the database
print("Writing to the database")
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database/products.db")
RSQLite::dbRemoveTable(my_connection,"products")
RSQLite::dbWriteTable(my_connection,"products",product_data)
RSQLite::dbDisconnect(my_connection)
print("Done")