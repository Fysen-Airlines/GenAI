import psycopg2
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import logging
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database connection settings
DATABASE_URL = "postgresql://aroy:zBkTCasbbGJiuZfESJcr@mizudb.cdccqagoo97x.us-east-1.rds.amazonaws.com/mizudb"

def get_db_connection():
    """Establish a database connection."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error("Error connecting to database: %s", e)
        return None

def create_tables():
    """Create necessary tables if they do not exist."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS optimized_results (
                route VARCHAR(10),
                optimized_price FLOAT,
                forecasted_demand FLOAT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS forecast_results (
                route VARCHAR(10),
                optimized_price FLOAT,
                forecasted_demand FLOAT,
                mean_squared_error FLOAT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        logging.info("Tables created or already exist.")
    except Exception as e:
        logging.error("Error creating tables: %s", e)
    finally:
        cursor.close()
        conn.close()

def fetch_historical_data(route):
    """Fetch historical data for a specific route."""
    conn = get_db_connection()
    if conn is None:
        return []

    try:
        cursor = conn.cursor()
        query = """
            SELECT date, tickets_sold, competitor_price, demand_index
            FROM historical_data 
            WHERE route = %s;
        """
        cursor.execute(query, (route,))
        data = cursor.fetchall()
        logging.info("Fetched historical data for route: %s", route)
    except Exception as e:
        logging.error("Error fetching historical data: %s", e)
        data = []
    finally:
        cursor.close()
        conn.close()

    return data

def fetch_all_routes():
    """Fetch all unique routes from the historical data."""
    conn = get_db_connection()
    if conn is None:
        return []

    try:
        cursor = conn.cursor()
        query = "SELECT DISTINCT route FROM historical_data;"
        cursor.execute(query)
        routes = [row[0] for row in cursor.fetchall()]
        logging.info("Fetched all routes from historical data.")
    except Exception as e:
        logging.error("Error fetching routes: %s", e)
        routes = []
    finally:
        cursor.close()
        conn.close()

    return routes

def insert_optimized_price(route, optimized_price, forecasted_demand):
    """Insert optimized price and forecasted demand into the database."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO optimized_results (route, optimized_price, forecasted_demand) 
            VALUES (%s, %s, %s);
        """, (route, float(optimized_price), float(forecasted_demand)))  # Convert to float
        conn.commit()
        logging.info("Inserted optimized price for route: %s", route)
    except Exception as e:
        logging.error("Error inserting optimized price: %s", e)
    finally:
        cursor.close()
        conn.close()

def insert_forecast_results(route, optimized_price, forecasted_demand, mse):
    """Insert forecast results into the database."""
    conn = get_db_connection()
    if conn is None:
        return

    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO forecast_results (route, optimized_price, forecasted_demand, mean_squared_error) 
            VALUES (%s, %s, %s, %s);
        """, (route, float(optimized_price), float(forecasted_demand), float(mse)))  # Convert to float
        conn.commit()
        logging.info("Inserted forecast results for route: %s", route)
    except Exception as e:
        logging.error("Error inserting forecast results: %s", e)
    finally:
        cursor.close()
        conn.close()

def dynamic_pricing_model(route):
    """Dynamic pricing model."""
    # Fetch historical data for the route
    historical_data = fetch_historical_data(route)

    # If no data is returned, log a warning and return
    if not historical_data:
        logging.warning("No historical data found for route: %s", route)
        return None

    # Convert historical data to a DataFrame
    df = pd.DataFrame(historical_data, columns=['date', 'tickets_sold', 'competitor_price', 'demand_index'])

    # Check if there are enough samples to proceed
    if df.shape[0] < 2:  # Not enough data to split into train and test sets
        logging.warning("Not enough data to run the model for route: %s", route)
        return None

    # Prepare data for training
    X = df[['competitor_price', 'demand_index']]
    y = df['tickets_sold']

    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train a linear regression model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Make predictions on the test set
    y_pred = model.predict(X_test)

    # Calculate mean squared error
    mse = mean_squared_error(y_test, y_pred)

    # Use the model to predict the optimized price
    input_data = pd.DataFrame([[100, 0.5]], columns=['competitor_price', 'demand_index'])  # Example input values with feature names
    optimized_price = model.predict(input_data)

    # Insert optimized price and forecasted demand into the database
    insert_optimized_price(route, optimized_price[0], 100)  # Example forecasted demand

    # Insert forecast results into the database
    insert_forecast_results(route, optimized_price[0], 100, mse)  # Example forecasted demand

    return optimized_price[0]

def main():
    # Create tables if they do not exist
    create_tables()

    # Fetch all unique routes
    routes = fetch_all_routes()

    # Run the dynamic pricing model for each route
    for route in routes:
        optimized_price = dynamic_pricing_model(route)
        if optimized_price is not None:
            logging.info("Optimized price for route %s: %s", route, optimized_price)

if __name__ == "__main__":
    main()