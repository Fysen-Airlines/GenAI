import psycopg2
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database connection settings
DATABASE_URL = "postgresql://user:password@host/mizudb"

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
        """, (route, optimized_price, float(forecasted_demand)))
        conn.commit()
        logging.info("Inserted optimized price for route: %s", route )
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
        # Convert numpy data types to native Python types (float)
        optimized_price = float(optimized_price)
        forecasted_demand = float(forecasted_demand)
        mse = float(mse)

        cursor.execute("""
            INSERT INTO forecast_results (route, optimized_price, forecasted_demand, mean_squared_error) 
            VALUES (%s, %s, %s, %s);
        """, (route, optimized_price, forecasted_demand, mse))
        conn.commit()
        logging.info("Inserted forecast results for route: %s", route)
    except Exception as e:
        logging.error("Error inserting forecast results: %s", e)
    finally:
        cursor.close()
        conn.close()


def forecast_demand(route):
    """Forecast demand for a given route."""
    historical_data = fetch_historical_data(route)

    if not historical_data:
        logging.warning("No historical data available for the route: %s", route)
        return None, None, None

    # Convert to DataFrame
    df = pd.DataFrame(historical_data, columns=['date', 'tickets_sold', 'competitor_price', 'demand_index'])

    # Feature engineering
    df['date'] = pd.to_datetime(df['date'])
    df['competitor_price'] = df['competitor_price'].astype(float)
    df['demand_index'] = df['demand_index'].astype(float)

    X = df[['competitor_price', 'demand_index']]
    y = df['tickets_sold']

    # Check the number of samples
    if len(df) < 2:  # Require at least 2 samples to perform the split
        logging.warning("Not enough data to train the model for route: %s", route)
        return None, None, None

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Predictions
    predictions = model.predict(X_test)

    # Evaluation
    mse = mean_squared_error(y_test, predictions)
    logging.info("Mean Squared Error for %s: %.2f", route, mse)

    forecasted_demand = model.predict(X).mean()  # Average forecast for all data
    return model, forecasted_demand, mse

def dynamic_pricing_model(current_price, demand_forecast, increase_percentage=0.10, decrease_percentage=0.10):
    """Apply dynamic pricing based on demand forecast."""
    if demand_forecast > 100:  # example threshold
        new_price = current_price * (1 + increase_percentage)  # Increase price
    else:
        new_price = current_price * (1 - decrease_percentage)  # Decrease price

    return round(new_price, 2)

def optimize_resources(routes):
    """Optimize resources for all routes."""
    for route in routes:
        model, forecasted_demand, mse = forecast_demand(route)
        if model and forecasted_demand is not None:
            current_price = 200.0  # Example current price

            # Create a DataFrame for the input features
            demand_index = 0.85  # Example demand index
            input_features = pd.DataFrame([[current_price, demand_index]], columns=['competitor_price', 'demand_index'])

            demand_forecast = model.predict(input_features)[0]  # Get the first element of the prediction

            optimized_price = dynamic_pricing_model(current_price, demand_forecast)

            # Insert the optimized price and forecasted demand into the respective tables
            insert_optimized_price(route, optimized_price, forecasted_demand)
            insert_forecast_results(route, optimized_price, forecasted_demand, mse)

# Main function
if __name__ == "__main__":
    create_tables()  # Create tables if they do not exist
    routes = fetch_all_routes()  # Fetch all routes from the database
    optimize_resources(routes)  # Optimize resources for all routes