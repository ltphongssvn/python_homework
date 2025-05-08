import sqlite3

def task1_complex_joins():
    # Task 1: Complex JOINs with Aggregation
    conn = sqlite3.connect('../db/lesson.db')
    cursor = conn.cursor()
    
    # SQL query to find total price of each of first 5 orders
    query = """
    SELECT o.order_id, SUM(p.price * li.quantity) AS total_price
    FROM orders o
    JOIN line_items li ON o.order_id = li.order_id
    JOIN products p ON li.product_id = p.product_id
    GROUP BY o.order_id
    ORDER BY o.order_id
    LIMIT 5;
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    print("\nTask 1: Complex JOINs with Aggregation")
    print("Order ID | Total Price")
    print("-" * 25)
    for row in results:
        print(f"{row[0]:8} | ${row[1]:.2f}")
    
    conn.close()


def task2_subqueries():
    # Task 2: Understanding Subqueries
    conn = sqlite3.connect('../db/lesson.db')
    cursor = conn.cursor()
    
    # SQL query with subquery to find average order price per customer
    query = """
    SELECT c.name, AVG(order_totals.total_price) AS average_total_price
    FROM customers c
    LEFT JOIN (
        SELECT o.customer_id AS customer_id_b, SUM(p.price * li.quantity) AS total_price
        FROM orders o
        JOIN line_items li ON o.order_id = li.order_id
        JOIN products p ON li.product_id = p.product_id
        GROUP BY o.order_id
    ) order_totals ON c.customer_id = order_totals.customer_id_b
    GROUP BY c.customer_id;
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    print("\nTask 2: Understanding Subqueries")
    print("Customer Name | Average Order Price")
    print("-" * 40)
    for row in results:
        print(f"{row[0]:15} | ${row[1]:.2f}")
    
    conn.close()


def task3_transaction():
    # Task 3: An Insert Transaction Based on Data
    conn = sqlite3.connect('../db/lesson.db')
    conn.execute("PRAGMA foreign_keys = 1")
    cursor = conn.cursor()
    
    try:
        # Start transaction
        conn.execute("BEGIN TRANSACTION")
        
        # Get customer_id for 'Perez and Sons'
        cursor.execute("SELECT customer_id FROM customers WHERE name = 'Perez and Sons'")
        customer_id = cursor.fetchone()[0]
        
        # Get employee_id for 'Miranda Harris'
        cursor.execute("SELECT employee_id FROM employees WHERE first_name = 'Miranda' AND last_name = 'Harris'")
        employee_id = cursor.fetchone()[0]
        
        # Get the 5 least expensive products
        cursor.execute("SELECT product_id FROM products ORDER BY price ASC LIMIT 5")
        product_ids = [row[0] for row in cursor.fetchall()]
        
        # Create order record
        cursor.execute(
            "INSERT INTO orders (customer_id, employee_id) VALUES (?, ?) RETURNING order_id",
            (customer_id, employee_id)
        )
        order_id = cursor.fetchone()[0]
        
        # Create line_item records
        for product_id in product_ids:
            cursor.execute(
                "INSERT INTO line_items (order_id, product_id, quantity) VALUES (?, ?, ?)",
                (order_id, product_id, 10)
            )
        
        # Commit transaction
        conn.commit()
        
        # Query to display the results
        query = """
        SELECT li.line_item_id, li.quantity, p.name
        FROM line_items li
        JOIN products p ON li.product_id = p.product_id
        WHERE li.order_id = ?
        """
        
        cursor.execute(query, (order_id,))
        results = cursor.fetchall()
        
        print("\nTask 3: An Insert Transaction Based on Data")
        print(f"New Order ID: {order_id}")
        print("Line Item ID | Quantity | Product Name")
        print("-" * 45)
        for row in results:
            print(f"{row[0]:12} | {row[1]:8} | {row[2]}")
        
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        print(f"Error: {e}")
    
    finally:
        conn.close()

def task4_having():
    # Task 4: Aggregation with HAVING
    conn = sqlite3.connect('../db/lesson.db')
    cursor = conn.cursor()
    
    # SQL query to find employees with more than 5 orders
    query = """
    SELECT e.employee_id, e.first_name, e.last_name, COUNT(o.order_id) AS order_count
    FROM employees e
    JOIN orders o ON e.employee_id = o.employee_id
    GROUP BY e.employee_id
    HAVING COUNT(o.order_id) > 5;
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    print("\nTask 4: Aggregation with HAVING")
    print("Employee ID | First Name | Last Name | Order Count")
    print("-" * 50)
    for row in results:
        print(f"{row[0]:11} | {row[1]:10} | {row[2]:9} | {row[3]}")
    
    conn.close()


if __name__ == "__main__":
    task1_complex_joins()
    task2_subqueries()
    task3_transaction()
    task4_having()