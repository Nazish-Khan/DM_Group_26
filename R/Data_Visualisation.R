library(patchwork)
library(ggplot2)
library(dplyr)
library(tidyverse)
library(RSQLite)


# Part 4: Data Analysis and Visualization


# Get the required data from the database

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
color_palette <- c("purple","pink","darkblue","blue","lightblue","black","darkgrey","white")

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


## Product Performance Analysis


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
top10_product <- ggplot(top_products, aes(x = total_quantity_sold, y = reorder(product_name, total_quantity_sold), fill = category_id)) +
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
bottom10_product <- ggplot(low_products, aes(x = total_quantity_sold, y = reorder(product_name, desc(total_quantity_sold)), fill = category_id)) +
  geom_bar(stat = "identity") +
  labs(title = "Below 5 Worst-Selling Products",
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

# Density plot
density_plot <- ggplot(result_df, aes(x = product_reviewscore, fill = order_status)) +
  geom_density(alpha = 0.8) +
  labs(title = "Density of Product Review Scores by Order Status",
       x = "Product Review Score",
       y = "Density",
       fill = "Order Status") +
  scale_fill_manual(values = color_palette) +
  theme_minimal()

# Print the plot
print(density_plot)


# Save plot figure
ggsave(filename = paste0(figure_directory, "selling_product_trend_", this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = combined_plot)
ggsave(filename = paste0(figure_directory, "rating_status_plot_", this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = density_plot)


## Sales Analysis




## Product-Category Analysis


# Calculate average review scores for each category
category_review_scores <- product %>%
  group_by(category_id) %>%
  summarise(average_review_score = mean(product_reviewscore, na.rm = TRUE))

# Join with 'CATEGORY' to get category names
category_review_scores <- category_review_scores %>%
  left_join(category, by = "category_id")

# Bar plot of average review scores by category
avg_review_plot <- ggplot(category_review_scores, aes(x = reorder(category_name, -average_review_score), y = average_review_score, fill = category_id)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = round(average_review_score, 2)), vjust = -0.5, color = "black", size = 3.5) + 
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

# Calculate profit margin
supply_product_df$profit_margin <- supply_product_df$product_price - supply_product_df$product_cost

# Check for duplicate column names
duplicate_columns1 <- anyDuplicated(names(supply_product_df)) > 0

# If there are duplicate column names, remove them
if (duplicate_columns1) {
  supply_product_df <- supply_product_df[, !duplicated(names(supply_product_df))]
}


# Margin Plot for the product wise profit average
margin_plot <- ggplot(supply_product_df, aes(x = category_id, y = profit_margin)) +
  geom_boxplot() +  # Boxplot to show distribution
  stat_summary(fun.y = mean, geom = "line", aes(group = 1), color = "red") +  # Average line
  stat_summary(fun.y = min, geom = "line", aes(group = 1), color = "green") +   # Minimum line
  stat_summary(fun.y = max, geom = "line", aes(group = 1), color = "black") + # Maximum line
  labs(title = "Profit Margin by Product",
       x = "Product Name",
       y = "Profit Margin") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))+ # Rotate x-axis labels
  scale_fill_manual(values = color_palette) +
  theme_minimal()

print(margin_plot)

# Save plot figure
ggsave(filename = paste0(figure_directory, "review_category_plot_", this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = avg_review_plot)
ggsave(filename = paste0(figure_directory, "profit_margin_plot_", this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = margin_plot)


## Customer Analysis


# Customer Segmentation

# Segment customers based on country
customers_country <- customer %>%
  group_by(customer_country) %>%
  summarise(total_customers = n())

# Plot for number of customers by country
cust_segm <- ggplot(customers_country, aes(x = customer_country, y = total_customers, fill = customer_country)) +
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

# Print the plot
print(payment_trend_plot)


# Save plot figure
ggsave(filename = paste0(figure_directory, "customer_segment_", this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = cust_segm)
ggsave(filename = paste0(figure_directory, "payment_trend_plot_", this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = payment_trend_plot)


## Ads vs Supplier Analysis


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
top5_suppliers_plot <- ggplot(top_suppliers, aes(x = avg_price_per_day, y = reorder(supplier_id, avg_price_per_day), fill = supplier_id)) +
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
bottom5_suppliers_plot <- ggplot(bottom_suppliers, aes(x = avg_price_per_day, y = reorder(supplier_id, avg_price_per_day), fill = supplier_id)) +
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

# Print the combined plot
print(combined_suppliers_plot)

# Save plot figure

ggsave(filename = paste0(figure_directory, "ads_supplier_plot_", this_filename_date, "_", this_filename_time, ".png"), width = 6, height = 5, plot = combined_suppliers_plot)
