import sqlite3
import pandas as pd
conn = sqlite3.connect("Data Engineer - Assignment Database.db")
query = """
SELECT
    s.customer_id AS Customer,
    c.age         AS Age,
    i.item_name   AS Item,
    CAST(SUM(o.quantity) AS INTEGER) AS Quantity
FROM customers c
JOIN sales     s ON s.customer_id = c.customer_id
JOIN orders    o ON o.sales_id    = s.sales_id
JOIN items     i ON i.item_id     = o.item_id
WHERE
    c.age BETWEEN 18 AND 35
    AND o.quantity IS NOT NULL
GROUP BY
    s.customer_id, c.age, i.item_name
HAVING
    SUM(o.quantity) > 0
ORDER BY
    Customer, Item;
"""

df_sql = pd.read_sql(query, conn)


df_sql.to_csv("output_sql.csv", sep=";", index=False)

print("Output file 'output_sql.csv' has been generated successfully.")




#----------------- Pandas Solution -----------------

customers = pd.read_sql("SELECT * FROM customers;", conn)
sales = pd.read_sql("SELECT * FROM sales;", conn)
orders = pd.read_sql("SELECT * FROM orders;", conn)
items = pd.read_sql("SELECT * FROM items;", conn)

df = (
    sales.merge(customers, on="customer_id", how="left")
         .merge(orders, on="sales_id", how="left")
         .merge(items, on="item_id", how="left")
)


df_prep = (
    df[(df["age"].between(18, 35)) & (df["quantity"].notna())]
    .groupby(["customer_id", "age", "item_name"], as_index=False)["quantity"]
    .sum()
)


df_prep = df_prep[df_prep["quantity"] > 0].copy()
df_prep["quantity"] = df_prep["quantity"].astype(int)

df_pandas = df_prep.rename(columns={
    "customer_id": "Customer",
    "age": "Age",
    "item_name": "Item",
    "quantity": "Quantity"
}).sort_values(["Customer", "Item"], kind="stable")


df_pandas.to_csv("output_pandas.csv", sep=";", index=False)

print("Output file 'output_pandas.csv' has been generated successfully.")


