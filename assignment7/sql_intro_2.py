# import sqlite3
# import pandas as pd

# # Connect to the database
# conn = sqlite3.connect("../db/lesson.db")

# # Step 2: Read data into a DataFrame
# query = """
# SELECT li.line_item_id, li.quantity, p.product_id, p.product_name, li.price
# FROM line_items li
# JOIN products p ON li.product_id = p.product_id
# """

# df = pd.read_sql_query(query, conn)

# # Step 3: Print first 5 lines
# print("Original DataFrame:")
# print(df.head())

# # Step 4: Add a 'total' column
# df['total'] = df['quantity'] * df['price']
# print("\nDataFrame with 'total' column:")
# print(df.head())

# # Step 5: Group by product_id
# summary_df = df.groupby('product_id').agg({
#     'line_item_id': 'count',
#     'total': 'sum',
#     'product_name': 'first'
# })
# print("\nGrouped DataFrame:")
# print(summary_df.head())

# # Step 6: Sort by product_name
# summary_df = summary_df.sort_values('product_name')
# print("\nSorted DataFrame:")
# print(summary_df.head())

# # Step 7: Write to CSV
# summary_df.to_csv('order_summary.csv')
# print("\nData written to order_summary.csv")

# # Close the connection
# conn.close()



import sqlite3
import pandas as pd

# Connect to the magazines database instead
conn = sqlite3.connect("../db/magazines.db")

# Query to get subscription data
query = """
SELECT s.subscription_id, s.subscriber_id, sb.name as subscriber_name, 
       m.magazine_id, m.name as magazine_name, s.expiration_date
FROM subscriptions s
JOIN subscribers sb ON s.subscriber_id = sb.subscriber_id
JOIN magazines m ON s.magazine_id = m.magazine_id
"""

# Read data into DataFrame
df = pd.read_sql_query(query, conn)

# Print first 5 lines
print("Original DataFrame:")
print(df.head())

# Create a "total" column (since we don't have price, we'll use 1 as dummy price)
df['price'] = 1  # Add dummy price
df['quantity'] = 1  # Add dummy quantity
df['total'] = df['quantity'] * df['price']
print("\nDataFrame with 'total' column:")
print(df.head())

# Group by magazine_id
summary_df = df.groupby('magazine_id').agg({
    'subscription_id': 'count',
    'total': 'sum',
    'magazine_name': 'first'
})
print("\nGrouped DataFrame:")
print(summary_df.head())

# Sort by magazine_name
summary_df = summary_df.sort_values('magazine_name')
print("\nSorted DataFrame:")
print(summary_df.head())

# Write to CSV
summary_df.to_csv('order_summary.csv')
print("\nData written to order_summary.csv")

conn.close()