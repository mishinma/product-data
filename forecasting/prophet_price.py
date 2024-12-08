import os
import sys

import duckdb
import matplotlib.pyplot as plt
from prophet import Prophet


LOCAL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DB_PATH = os.path.join(LOCAL_DIR, "sim", "products.duckdb")
INPUT_MODEL_NAME = "ml_prophet_price_forecast_input"
INFO_MODEL_NAME = "intm_latest_products"


def print_product_info(product_id, con):
    """
    Fetch and print detailed product information from the intm_latest_products table.

    Args:
        product_id (int): The product ID to fetch information for.
        con: The DuckDB connection object.
    """

    # Fetch product information
    query = f"""
    SELECT *
    FROM intm_latest_products
    WHERE product_id = {product_id}
    """
    product_info = con.execute(query).fetchdf()

    if product_info.empty:
        print(f"No information found for product_id {product_id}.")
        return

    # Print product information
    print("Product Information:")
    print(product_info.to_string(index=False))


def forecast_price(product_id, db_path=DB_PATH, model_name=INPUT_MODEL_NAME):
    """
    Forecast the price of a product for the next 30 days using Prophet and DuckDB.

    Args:
        product_id (int): The product ID to forecast.
        db_path (str): Path to the DuckDB database file.

    Returns:
        DataFrame: A DataFrame with the forecasted prices for the next 30 days.
    """
    # Connect to DuckDB
    con = duckdb.connect(db_path)

    # Fetch historical data for the given product ID
    query = f"""
    SELECT ds, y
    FROM {model_name}
    WHERE product_id = {product_id}
    ORDER BY ds;
    """
    df = con.execute(query).fetchdf()

    if df.empty:
        print(f"No historical data found for product_id {product_id}.")
        return

    # Fetch and print detailed product information
    print_product_info(product_id, con)

    # Train Prophet model
    model = Prophet()
    model.fit(df)

    # Create future dates for forecasting
    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)

    # Plot the forecast
    plt.figure(figsize=(10, 6))
    model.plot(forecast)
    plt.title(f"Price Forecast for Product ID {product_id}")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.grid()
    plt.show()

    # Filter only the required columns
    forecast = forecast[["ds", "yhat"]].rename(columns={"ds": "date", "yhat": "forecasted_price"})
    forecast["product_id"] = product_id

    # Print the forecasted prices
    print(f"Forecast for product_id {product_id} for the next 30 days:")
    print(forecast.tail(30))

    return forecast


def main():
    # Check if product ID is provided
    if len(sys.argv) != 2:
        print("Usage: python forecast_price_duckdb.py <product_id>")
        sys.exit(1)

    # Parse arguments
    product_id = int(sys.argv[1])

    # Forecast price for the given product ID
    forecast_price(product_id)


if __name__ == "__main__":
    main()
