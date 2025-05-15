# import sqlite3

# try:
#     with sqlite3.connect("../db/magazines.db") as conn:
#         # Enable foreign key constraints
#         conn.execute("PRAGMA foreign_keys = 1")
        
#         cursor = conn.cursor()
        
#         # Create publishers table
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS publishers (
#             publisher_id INTEGER PRIMARY KEY,
#             name TEXT NOT NULL UNIQUE
#         )
#         """)
        
#         # Create magazines table (with foreign key to publishers)
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS magazines (
#             magazine_id INTEGER PRIMARY KEY,
#             name TEXT NOT NULL UNIQUE,
#             publisher_id INTEGER,
#             FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id)
#         )
#         """)
        
#         # Create subscribers table
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS subscribers (
#             subscriber_id INTEGER PRIMARY KEY,
#             name TEXT NOT NULL,
#             address TEXT NOT NULL
#         )
#         """)
        
#         # Create subscriptions join table
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS subscriptions (
#             subscription_id INTEGER PRIMARY KEY,
#             subscriber_id INTEGER,
#             magazine_id INTEGER,
#             expiration_date TEXT NOT NULL,
#             FOREIGN KEY (subscriber_id) REFERENCES subscribers(subscriber_id),
#             FOREIGN KEY (magazine_id) REFERENCES magazines(magazine_id)
#         )
#         """)
        
#         print("Tables created successfully.")
        
# except sqlite3.Error as e:
#     print(f"Error connecting to database: {e}")


import sqlite3


def add_publisher(conn, name):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM publishers WHERE name = ?", (name,))
        if cursor.fetchone():
            print(f"Publisher {name} already exists")
            return False
        cursor.execute("INSERT INTO publishers (name) VALUES (?)", (name,))
        return True
    except Exception as e:
        print(f"Error adding publisher: {e}")
        return False


def add_magazine(conn, name, publisher_id):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM magazines WHERE name = ?", (name,))
        if cursor.fetchone():
            print(f"Magazine {name} already exists")
            return False
        cursor.execute("INSERT INTO magazines (name, publisher_id) VALUES (?, ?)", 
                     (name, publisher_id))
        return True
    except Exception as e:
        print(f"Error adding magazine: {e}")
        return False


def add_subscriber(conn, name, address):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM subscribers WHERE name = ? AND address = ?", 
                     (name, address))
        if cursor.fetchone():
            print(f"Subscriber {name} at {address} already exists")
            return False
        cursor.execute("INSERT INTO subscribers (name, address) VALUES (?, ?)", 
                     (name, address))
        return True
    except Exception as e:
        print(f"Error adding subscriber: {e}")
        return False


def query_all_subscribers(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM subscribers")
    subscribers = cursor.fetchall()
    print("\nAll Subscribers:")
    for subscriber in subscribers:
        print(subscriber)
    return subscribers

def query_magazines_by_name(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM magazines ORDER BY name")
    magazines = cursor.fetchall()
    print("\nMagazines sorted by name:")
    for magazine in magazines:
        print(magazine)
    return magazines

def query_magazines_by_publisher(conn, publisher_name):
    cursor = conn.cursor()
    cursor.execute("""
    SELECT m.magazine_id, m.name, p.name as publisher_name
    FROM magazines m
    JOIN publishers p ON m.publisher_id = p.publisher_id
    WHERE p.name = ?
    """, (publisher_name,))
    magazines = cursor.fetchall()
    print(f"\nMagazines published by {publisher_name}:")
    for magazine in magazines:
        print(magazine)
    return magazines


def add_subscription(conn, subscriber_id, magazine_id, expiration_date):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM subscriptions WHERE subscriber_id = ? AND magazine_id = ?", 
                     (subscriber_id, magazine_id))
        if cursor.fetchone():
            print(f"Subscription already exists")
            return False
        cursor.execute("INSERT INTO subscriptions (subscriber_id, magazine_id, expiration_date) VALUES (?, ?, ?)", 
                     (subscriber_id, magazine_id, expiration_date))
        return True
    except Exception as e:
        print(f"Error adding subscription: {e}")
        return False

try:
    with sqlite3.connect("../db/magazines.db") as conn:
        # Enable foreign key constraints
        conn.execute("PRAGMA foreign_keys = 1")
        
        cursor = conn.cursor()
        
        # Create publishers table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS publishers (
            publisher_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """)
        
        # Create magazines table (with foreign key to publishers)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS magazines (
            magazine_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            publisher_id INTEGER,
            FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id)
        )
        """)
        
        # Create subscribers table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            subscriber_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT NOT NULL
        )
        """)
        
        # Create subscriptions join table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            subscription_id INTEGER PRIMARY KEY,
            subscriber_id INTEGER,
            magazine_id INTEGER,
            expiration_date TEXT NOT NULL,
            FOREIGN KEY (subscriber_id) REFERENCES subscribers(subscriber_id),
            FOREIGN KEY (magazine_id) REFERENCES magazines(magazine_id)
        )
        """)
        
        print("Database created and connected successfully.")
        
        # Add publishers
        add_publisher(conn, "Conde Nast")
        add_publisher(conn, "Hearst")
        add_publisher(conn, "Meredith")
        
        # Add magazines (get publisher_ids first)
        cursor = conn.cursor()
        cursor.execute("SELECT publisher_id FROM publishers WHERE name = 'Conde Nast'")
        conde_id = cursor.fetchone()[0]
        cursor.execute("SELECT publisher_id FROM publishers WHERE name = 'Hearst'")
        hearst_id = cursor.fetchone()[0]
        cursor.execute("SELECT publisher_id FROM publishers WHERE name = 'Meredith'")
        meredith_id = cursor.fetchone()[0]
        
        add_magazine(conn, "Vogue", conde_id)
        add_magazine(conn, "GQ", conde_id)
        add_magazine(conn, "Cosmopolitan", hearst_id)
        add_magazine(conn, "Better Homes & Gardens", meredith_id)
        
        # Add subscribers
        add_subscriber(conn, "John Smith", "123 Main St")
        add_subscriber(conn, "Jane Doe", "456 Oak Ave")
        add_subscriber(conn, "Bob Johnson", "789 Pine Rd")
        
        # Add subscriptions (get IDs first)
        cursor.execute("SELECT subscriber_id FROM subscribers WHERE name = 'John Smith'")
        john_id = cursor.fetchone()[0]
        cursor.execute("SELECT subscriber_id FROM subscribers WHERE name = 'Jane Doe'")
        jane_id = cursor.fetchone()[0]
        cursor.execute("SELECT subscriber_id FROM subscribers WHERE name = 'Bob Johnson'")
        bob_id = cursor.fetchone()[0]
        
        cursor.execute("SELECT magazine_id FROM magazines WHERE name = 'Vogue'")
        vogue_id = cursor.fetchone()[0]
        cursor.execute("SELECT magazine_id FROM magazines WHERE name = 'Cosmopolitan'")
        cosmo_id = cursor.fetchone()[0]
        cursor.execute("SELECT magazine_id FROM magazines WHERE name = 'GQ'")
        gq_id = cursor.fetchone()[0]
        
        add_subscription(conn, john_id, vogue_id, "2023-12-31")
        add_subscription(conn, jane_id, cosmo_id, "2023-10-15")
        add_subscription(conn, bob_id, gq_id, "2024-01-01")

        # Execute queries
        query_all_subscribers(conn)
        query_magazines_by_name(conn)
        query_magazines_by_publisher(conn, "Conde Nast")  # Use one of your publishers
        
        # Commit the changes
        conn.commit()
        print("Tables populated successfully.")
        
except sqlite3.Error as e:
    print(f"Error connecting to database: {e}")