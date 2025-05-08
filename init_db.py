import sqlite3

conn = sqlite3.connect("db/lesson.db")
cursor = conn.cursor()

# Create tables
cursor.executescript('''
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS employees (
    employee_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    employee_id INTEGER,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

CREATE TABLE IF NOT EXISTS line_items (
    line_item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Clear existing data
DELETE FROM line_items;
DELETE FROM orders;
DELETE FROM products;
DELETE FROM customers;
DELETE FROM employees;

-- Insert sample data
-- Customers
INSERT INTO customers VALUES (1, 'Acme Corp');
INSERT INTO customers VALUES (2, 'Perez and Sons');
INSERT INTO customers VALUES (3, 'Global Enterprises');

-- Employees
INSERT INTO employees VALUES (1, 'John', 'Smith');
INSERT INTO employees VALUES (2, 'Miranda', 'Harris');
INSERT INTO employees VALUES (3, 'Emily', 'Johnson');

-- Products
INSERT INTO products VALUES (1, 'Widget', 10.99);
INSERT INTO products VALUES (2, 'Gadget', 25.50);
INSERT INTO products VALUES (3, 'Tool', 15.75);
INSERT INTO products VALUES (4, 'Component', 8.25);
INSERT INTO products VALUES (5, 'Accessory', 5.99);
INSERT INTO products VALUES (6, 'Module', 30.00);

-- Orders (adding more so some employees have >5 orders)
INSERT INTO orders VALUES (1, 1, 1);
INSERT INTO orders VALUES (2, 1, 2);
INSERT INTO orders VALUES (3, 2, 1);
INSERT INTO orders VALUES (4, 3, 3);
INSERT INTO orders VALUES (5, 2, 2);
INSERT INTO orders VALUES (6, 1, 3);
INSERT INTO orders VALUES (7, 3, 1);
INSERT INTO orders VALUES (8, 2, 3);
-- Additional orders for employee 1 (John Smith)
INSERT INTO orders VALUES (9, 1, 1);
INSERT INTO orders VALUES (10, 2, 1);
INSERT INTO orders VALUES (11, 3, 1);
INSERT INTO orders VALUES (12, 1, 1);
-- Additional orders for employee 2 (Miranda Harris)
INSERT INTO orders VALUES (13, 3, 2);
INSERT INTO orders VALUES (14, 1, 2);
INSERT INTO orders VALUES (15, 2, 2);
INSERT INTO orders VALUES (16, 3, 2);

-- Line Items
INSERT INTO line_items VALUES (1, 1, 1, 5);
INSERT INTO line_items VALUES (2, 1, 2, 2);
INSERT INTO line_items VALUES (3, 2, 3, 10);
INSERT INTO line_items VALUES (4, 2, 4, 3);
INSERT INTO line_items VALUES (5, 3, 1, 7);
INSERT INTO line_items VALUES (6, 3, 5, 4);
INSERT INTO line_items VALUES (7, 4, 6, 2);
INSERT INTO line_items VALUES (8, 4, 2, 5);
INSERT INTO line_items VALUES (9, 5, 3, 3);
INSERT INTO line_items VALUES (10, 5, 4, 8);
-- Add line items for new orders
INSERT INTO line_items VALUES (11, 6, 1, 4);
INSERT INTO line_items VALUES (12, 7, 2, 6);
INSERT INTO line_items VALUES (13, 8, 3, 2);
INSERT INTO line_items VALUES (14, 9, 4, 7);
INSERT INTO line_items VALUES (15, 10, 5, 3);
INSERT INTO line_items VALUES (16, 11, 6, 5);
INSERT INTO line_items VALUES (17, 12, 1, 8);
INSERT INTO line_items VALUES (18, 13, 2, 4);
INSERT INTO line_items VALUES (19, 14, 3, 6);
INSERT INTO line_items VALUES (20, 15, 4, 2);
INSERT INTO line_items VALUES (21, 16, 5, 5);
''')

conn.commit()
conn.close()

print("Database initialized successfully with more orders for Task 4.")