"""
This module simulates a data ingestion process for product price adjustments over time,
based on category-specific rules. It fetches product data, adjusts prices dynamically
for a simulated past year, and stores the results in a DuckDB database.

Usage:
    Run this script to generate and ingest simulated product data for the past year,
    adjusted dynamically based on predefined category rules.
"""

import csv
import os
import random
from datetime import datetime, timedelta

from product_data.main import fetch_data, ingest_data


# File paths
LOCAL_DIR = os.path.join(os.path.dirname(__file__), "data")
VALID_DATA_DIR = os.path.join(LOCAL_DIR, "valid")
INVALID_DATA_DIR = os.path.join(LOCAL_DIR, "invalid")
INPUT_CSV = os.path.join(VALID_DATA_DIR, "products.csv")
DB_DIR = os.path.join(LOCAL_DIR, "sim")
DB_FILE = os.path.join(DB_DIR, "products.duckdb")


# Price adjustment rules
CATEGORY_ADJUSTMENTS = {
    "electronics": 0.1,  # Decrease by 10% over the year
    "men's clothing": -0.1,  # Increase by 10% over the year
    "women's clothing": -0.1,  # Increase by 10% over the year
    "jewelry": -0.2,  # Increase by 20% over the year
}


# Adjust price based on category and day
def adjust_price(base_price, category, days_ago):
    if days_ago == 0:
        return base_price
    adjustment = CATEGORY_ADJUSTMENTS.get(category, 0)
    daily_adjustment = adjustment / 365
    noise = random.uniform(-0.005, 0.005)
    adjusted_price = base_price * (1 + daily_adjustment * days_ago + noise)
    print(f"Adjusted price for {category} on day {days_ago}: {adjusted_price}")
    return round(adjusted_price, 2)


# Main script
def main():
    # Create the data directories if they don't exist
    for dir_path in [VALID_DATA_DIR, INVALID_DATA_DIR, DB_DIR]:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    # Drop the database if it exists
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    # Fetch data from the api
    fetch_data(valid_data_dir=VALID_DATA_DIR, invalid_data_dir=INVALID_DATA_DIR)

    # Read the base CSV file
    with open(INPUT_CSV) as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    # Extract base price for each product
    base_prices = {}
    for row in rows:
        base_prices[row["id"]] = float(row["price"])

    # Loop through each day in the past year
    today = datetime.now()
    for days_ago in range(365):
        current_date = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Processing data for {current_date}, {days_ago} days ago...")
        temp_csv = os.path.join(LOCAL_DIR, "sim_products.csv")

        # Adjust the data for the current day
        with open(temp_csv, "w", newline="") as temp_file:
            writer = csv.DictWriter(temp_file, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in rows:
                row["loaded_at"] = current_date
                base_price = base_prices[row["id"]]
                row["price"] = adjust_price(base_price, row["category"], days_ago)
                writer.writerow(row)

        # Ingest data into DuckDB
        ingest_data(database_file=DB_FILE, file_path=temp_csv)

    # remove the temporary file
    os.remove(temp_csv)
    print("Data simulation complete.")


if __name__ == "__main__":
    main()
