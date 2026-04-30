import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_ORDERS = 5000

print("Generating sample eCommerce data...")

# Generate Customers
print("Creating customers...")
customers = pd.DataFrame({
    'customer_id': range(1, NUM_CUSTOMERS + 1),
    'first_name': [random.choice(['John', 'Jane', 'Mike', 'Sarah', 'David', 'Emily', 'Chris', 'Anna', 'James', 'Lisa']) for _ in range(NUM_CUSTOMERS)],
    'last_name': [random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']) for _ in range(NUM_CUSTOMERS)],
    'email': [f'customer{i}@example.com' for i in range(1, NUM_CUSTOMERS + 1)],
    'country': np.random.choice(['USA', 'Canada', 'UK', 'Germany', 'France', 'India', 'Australia'], NUM_CUSTOMERS, p=[0.4, 0.15, 0.15, 0.1, 0.1, 0.05, 0.05]),
    'signup_date': [datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730)) for _ in range(NUM_CUSTOMERS)]
})

# Generate Products
print("Creating products...")
categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food']
products = pd.DataFrame({
    'product_id': range(1, NUM_PRODUCTS + 1),
    'product_name': [f'Product {i}' for i in range(1, NUM_PRODUCTS + 1)],
    'category': np.random.choice(categories, NUM_PRODUCTS),
    'price': np.round(np.random.uniform(10, 500, NUM_PRODUCTS), 2),
    'cost': np.round(np.random.uniform(5, 250, NUM_PRODUCTS), 2)
})

# Generate Orders
print("Creating orders...")
order_dates = [datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365)) for _ in range(NUM_ORDERS)]
orders = pd.DataFrame({
    'order_id': range(1, NUM_ORDERS + 1),
    'customer_id': np.random.randint(1, NUM_CUSTOMERS + 1, NUM_ORDERS),
    'order_date': order_dates,
    'order_status': np.random.choice(['completed', 'pending', 'cancelled', 'refunded'], NUM_ORDERS, p=[0.85, 0.08, 0.05, 0.02])
})

# Generate Order Items (1-5 items per order)
print("Creating order items...")
order_items = []
for order_id in range(1, NUM_ORDERS + 1):
    num_items = random.randint(1, 5)
    for _ in range(num_items):
        product_id = random.randint(1, NUM_PRODUCTS)
        
        # Get product price safely
        product_row = products[products['product_id'] == product_id]
        if len(product_row) > 0:
            product_price = product_row['price'].values[0]
        else:
            product_price = 50.0
        
        quantity = random.randint(1, 3)
        
        order_items.append({
            'order_item_id': len(order_items) + 1,
            'order_id': order_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price': product_price,
            'total_price': round(quantity * product_price, 2)
        })

order_items_df = pd.DataFrame(order_items)

# Save to CSV
print("Saving CSV files...")
customers.to_csv('data/raw_customers.csv', index=False)
products.to_csv('data/raw_products.csv', index=False)
orders.to_csv('data/raw_orders.csv', index=False)
order_items_df.to_csv('data/raw_order_items.csv', index=False)

print(f"\n✅ Data generation complete!")
print(f"   - {len(customers)} customers")
print(f"   - {len(products)} products")
print(f"   - {len(orders)} orders")
print(f"   - {len(order_items_df)} order items")
print(f"\nFiles saved in 'data/' directory")