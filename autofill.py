import psycopg2
from typing import Optional
from collections import Counter
import random  # To generate seat numbers

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="mizudb.cdccqagoo97x.us-east-1.rds.amazonaws.com",
    database="mizudb",
    user="aroy",
    password="zBkTCasbbGJiuZfESJcr",
)

# Create a cursor object
cur = conn.cursor()

# Create a new table for customer suggestions if it doesn't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS customer_suggestions (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        email VARCHAR(255),
        seat_suggestion TEXT,
        meal_suggestion TEXT,
        most_common_route TEXT,
        assigned_seat_number VARCHAR(10)  -- Add seat number column
    );
""")
conn.commit()

# Function to fetch all customer profiles
def fetch_all_customer_profiles():
    cur.execute(""" 
        SELECT name, email, frequent_routes, seat_preference, meal_preference, phone_number 
        FROM customer_profiles; 
    """)
    return cur.fetchall()

# Function to get the most common route from frequent_routes
def get_most_common_route(frequent_routes: str):
    # Split the routes by comma and strip whitespace
    routes = [route.strip() for route in frequent_routes.split(",")]
    # Use Counter to find the most common route
    if routes:
        most_common_route, _ = Counter(routes).most_common(1)[0]
        return most_common_route
    return None

# Function to assign a seat number based on preference
def assign_seat_number(seat_preference: str):
    row_numbers = list(range(1, 31))
    random.shuffle(row_numbers)

    if seat_preference == "aisle":
        # For aisle seats, use letters A, C, E, ...
        seat_letters = ['A', 'C', 'E', 'G', 'J']
    else:
        # For window seats, use letters B, D, F, ...
        seat_letters = ['B', 'D', 'F', 'H', 'K']

    random.shuffle(seat_letters)

    row_number = row_numbers.pop()
    seat_letter = seat_letters.pop()

    return f"{row_number}{seat_letter}"

# Simple AI-based suggestion system (for demonstration)
def ai_suggestions(customer_profile):
    if not customer_profile:
        return "Customer profile not found."

    name, email, frequent_routes, seat_preference, meal_preference, phone_number = customer_profile
    first_name, last_name = name.split(maxsplit=1)  # Split name into first and last

    # Generate suggestions based on customer data
    suggestions = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "seat_suggestion": "aisle" if seat_preference == "aisle" else "window",
        "meal_suggestion": "vegetarian" if meal_preference == "vegetarian" else "non-vegetarian",
        "most_common_route": get_most_common_route(frequent_routes),
        "assigned_seat_number": assign_seat_number(seat_preference)
    }

    return suggestions

# Function to save suggestions to the database
def save_suggestions_to_db(suggestions):
    cur.execute("""
        INSERT INTO customer_suggestions (first_name, last_name, email, seat_suggestion, meal_suggestion, most_common_route, assigned_seat_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (suggestions['first_name'], suggestions['last_name'], suggestions['email'], suggestions['seat_suggestion'], suggestions['meal_suggestion'], suggestions['most_common_route'], suggestions['assigned_seat_number']))
    conn.commit()

# Fetch all customer profiles from the database
customer_profiles = fetch_all_customer_profiles()

# Process each customer profile
for customer_profile in customer_profiles:
    # Generate suggestions based on the profile
    suggestions = ai_suggestions(customer_profile)

    # Print the suggestions and save them to the database
    if isinstance(suggestions, str):
        print(suggestions)  # In case of error or no profile found
    else:
        print(f"Suggestions for {suggestions['email']}:")
        print(f"Seat Preference: {suggestions['seat_suggestion']}")
        print(f"Meal Preference: {suggestions['meal_suggestion']}")
        if suggestions['most_common_route']:
            print(f"Most Common Route: {suggestions['most_common_route']}")
        else:
            print("No frequent routes found.")
        print(f"Assigned Seat Number: {suggestions['assigned_seat_number']}")  # Print assigned seat number

        # Save suggestions to the database
        save_suggestions_to_db(suggestions)

# Close the cursor and connection
cur.close()
conn.close()