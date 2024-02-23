library(RSQLite)
library(readr)

# Reading the csv file
product_data <- readr::read_csv("data_upload/MOCK_DATA_PRODUCTS.csv")

# Writing the data to the database
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database/products.db")
RSQLite::dbWriteTable(my_connection,"products",product_data)
RSQLite::dbDisconnect()