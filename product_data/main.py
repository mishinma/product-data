import json
import os
from datetime import datetime

import duckdb
import pandas as pd
import requests
from jsonschema import ValidationError, validate


# API endpoint
API_URL = "https://fakestoreapi.com/products"
LOCAL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
VALID_PRODUCTS_DIR = os.path.join(LOCAL_DIR, "valid")
INVALID_PRODUCTS_DIR = os.path.join(LOCAL_DIR, "invalid")
DATABASE_FILE = os.path.join(LOCAL_DIR, "products.duckdb")
TABLE_NAME = "raw_products"
TEMP_TABLE_NAME = "temp_" + TABLE_NAME

# JSON Schema for validation
PRODUCT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "title": {"type": "string"},
        "price": {"type": "number"},
        "description": {"type": "string"},
        "image": {"type": "string"},
        "category": {"type": "string"},
        "rating": {"type": "object", "properties": {"rate": {"type": "number"}, "count": {"type": "integer"}}},
    },
    "required": ["id", "title", "price"],
}

# Define the table schema as a static array
TABLE_SCHEMA = [
    {"name": "id", "type": "INTEGER", "constraints": "NOT NULL"},
    {"name": "title", "type": "TEXT", "constraints": "NOT NULL"},
    {"name": "price", "type": "FLOAT", "constraints": "NOT NULL"},
    {"name": "category", "type": "TEXT", "constraints": ""},
    {"name": "description", "type": "TEXT", "constraints": ""},
    {"name": "image", "type": "TEXT", "constraints": ""},
    {"name": "rating_rate", "type": "FLOAT", "constraints": ""},
    {"name": "rating_count", "type": "INTEGER", "constraints": ""},
    {"name": "loaded_at", "type": "TIMESTAMP", "constraints": ""},
    {"name": "_synched", "type": "TIMESTAMP", "constraints": "DEFAULT CURRENT_TIMESTAMP"},
]
# Temp table schema is the same, but without the _synched column and with
# PRIMARY KEY constraint on the id column
TEMP_TABLE_SCHEMA = [column.copy() for column in TABLE_SCHEMA if column["name"] != "_synched"]
TEMP_TABLE_SCHEMA[0]["constraints"] = "PRIMARY KEY"


def generate_create_table_statement(table_name, schema, if_not_exists=True, temporary=False):
    """
    Generate a CREATE TABLE statement based on the provided schema.
    """
    columns = []
    for column in schema:
        column_def = f"{column['name'].upper()} {column['type']} {column['constraints']}".strip()
        columns.append(column_def)
    columns_str = ",\n    ".join(columns)

    if_not_exists_str = "IF NOT EXISTS" if if_not_exists else ""
    temporary_str = "TEMP" if temporary else ""

    return f"CREATE {temporary_str} TABLE {if_not_exists_str} {table_name} (\n    {columns_str}\n);"


def get_column_order(schema):
    """
    Extract column names from the schema in the defined order.
    """
    return [column["name"] for column in schema]


def fetch_data():
    # Fetch data from the API
    response = requests.get(API_URL)
    if response.status_code == 200:
        products = response.json()
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        exit()

    print(f"Number of products fetched: {len(products)}")

    # Create local directories if they don't exist
    for dir_path in [VALID_PRODUCTS_DIR, INVALID_PRODUCTS_DIR]:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    # Generate a timestamp prefix
    now = datetime.now()
    timestamp_int = int(now.timestamp())
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    valid_records = []
    invalid_records = []

    # # TEST: Invalidate some records
    # products[0]["price"] = "invalid"
    # products[1]["rating"]["rate"] = "invalid"
    # products[2]["title"] = None
    # # This will actually break ingestion as we have a primary key constraint
    # # We can also check the records for duplicate IDs and move them to invalid
    # # products[3]["id"] = 5
    # # These should be still valid
    # products[3]["rating"].pop("count")
    # products[4]["rating"].pop("rate")
    # products[5].pop("rating")

    # Validate each record against the schema
    for product in products:
        try:
            validate(instance=product, schema=PRODUCT_SCHEMA)
            product["loaded_at"] = timestamp_str
            product["rating_rate"] = product.get("rating", {}).get("rate", None)
            product["rating_count"] = product.get("rating", {}).get("count", None)
            product.pop("rating", None)  # Remove the nested rating object
            valid_records.append(product)
        except ValidationError as e:
            print(f"Invalid record: {e.message}")
            invalid_records.append(product)

    # Extract the column order
    column_order = get_column_order(TEMP_TABLE_SCHEMA)

    # Save valid records to CSV
    valid_csv_file = os.path.join(VALID_PRODUCTS_DIR, "products.csv")
    try:
        if valid_records:
            df_valid = pd.DataFrame(valid_records)
            # Ensure all columns exist and are in the correct order
            df_valid = df_valid.reindex(columns=column_order, fill_value=None)
            df_valid.to_csv(valid_csv_file, index=False)
            print(f"Valid data saved to {valid_csv_file}")
        else:
            print("No valid records to save.")
    except Exception as e:
        print(f"An error occurred while saving valid data to CSV: {e}")

    # Save all invalid records into a single JSON file
    invalid_file = os.path.join(INVALID_PRODUCTS_DIR, f"{timestamp_int}_products_invalid.json")
    try:
        if invalid_records:
            with open(invalid_file, "w") as f:
                json.dump(invalid_records, f, indent=4)
            print(f"Invalid data saved to {invalid_file}")
        else:
            print("No invalid records to save.")
    except Exception as e:
        print(f"An error occurred while saving invalid data to JSON: {e}")


def ingest_data():
    # Connect to DuckDB and create a table
    with duckdb.connect(DATABASE_FILE) as conn:
        # Create the main table if it doesn't exist
        create_table_query = generate_create_table_statement(TABLE_NAME, TABLE_SCHEMA, if_not_exists=True)
        print(create_table_query)
        conn.execute(create_table_query)

        # Create a temp table
        create_temp_table_query = generate_create_table_statement(
            TEMP_TABLE_NAME, TEMP_TABLE_SCHEMA, if_not_exists=False, temporary=True
        )
        conn.execute(create_temp_table_query)

        # Load the data into the temp table in DuckDB
        file_path = os.path.join(VALID_PRODUCTS_DIR, "products.csv")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            raise FileNotFoundError

        copy_query = f"""
        COPY {TEMP_TABLE_NAME} FROM '{file_path}' (HEADER);
        """
        try:
            res = conn.execute(copy_query)
        except Exception as e:
            print(f"An error occurred while loading data into DuckDB: {e}")
            raise e
        rows_loaded = res.fetchone()[0]
        print("Data loaded into DuckDB.")
        print(f"Number of rows loaded: {rows_loaded}")

        # Insert new records into the main table
        insert_query = f"""
            INSERT INTO {TABLE_NAME}
            SELECT *, CURRENT_TIMESTAMP AS _synched
            FROM {TEMP_TABLE_NAME};
        """
        conn.execute(insert_query)
        print("New records inserted into the main table.")

        # Describe the table
        table_info = conn.execute("DESCRIBE raw_products").fetchall()
        print("Table schema:")
        header = [desc[0] for desc in conn.description]
        print(header)
        for row in table_info:
            print(row)

        # Return the number of rows
        row_count = conn.execute("SELECT COUNT(*) FROM raw_products").fetchone()
        print(f"Number of rows: {row_count[0]}")


def main():
    fetch_data()
    ingest_data()


if __name__ == "__main__":
    main()
