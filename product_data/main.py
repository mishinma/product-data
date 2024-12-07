import requests
import json
import duckdb
import os

# API endpoint
API_URL = "https://fakestoreapi.com/products"
LOCAL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DATABASE_FILE = os.path.join(LOCAL_DIR, "products.duckdb")


def fetch_data():
    # Fetch data from the API
    response = requests.get(API_URL)
    if response.status_code == 200:
        products = response.json()
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        exit()

    # Create a local directory if it doesn't exist
    if not os.path.exists(LOCAL_DIR):
        os.makedirs(LOCAL_DIR)

    # Print the number of products
    print(f"Number of products fetched: {len(products)}")

    # Save data to a local JSON file
    json_file = os.path.join(LOCAL_DIR, "products.json")
    with open(json_file, "w") as file:
        json.dump(products, file, indent=4)
    print(f"Data saved to {json_file}")


def ingest_data():
    # Connect to DuckDB and create a table
    with duckdb.connect(DATABASE_FILE) as conn:
        create_table_query = """
        CREATE OR REPLACE table products (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            price FLOAT NOT NULL CHECK(price >= 0),
            category TEXT NOT NULL,
            description TEXT,
            image TEXT,
            rating STRUCT(rate FLOAT, count INTEGER)
        )
        """  # ToDo: add loaded at timestamp
        conn.execute(create_table_query)

        # Load the data into DuckDB
        file_path = os.path.join(LOCAL_DIR, "products.json")
        copy_query = f"""
        COPY products FROM '{file_path}' (FORMAT JSON, ARRAY true)
        """
        res = conn.execute(copy_query)
        rows_loaded = res.fetchone()[0]
        print("Data loaded into DuckDB.")
        print(f"Number of rows loaded: {rows_loaded}")

        # Describe the table
        table_info = conn.execute("DESCRIBE products").fetchall()
        print("Table schema:")
        header = [desc[0] for desc in conn.description]
        print(header)
        for row in table_info:
            print(row)

        # Return the number of rows
        row_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()
        print(f"Number of rows: {row_count[0]}")

        # Verify the data was loaded
        result = conn.execute("SELECT * FROM products LIMIT 1").fetchall()
        print("Sample data from DuckDB:")
        for row in result:
            print(row)


def main():
    fetch_data()
    ingest_data()


if __name__ == "__main__":
    main()
