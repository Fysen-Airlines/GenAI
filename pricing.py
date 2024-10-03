import psycopg2
from psycopg2 import sql
import requests
import datetime


# Database connection setup
def get_db_cursor():
    conn = psycopg2.connect(
        host="mizudb.cdccqagoo97x.us-east-1.rds.amazonaws.com",
        database="mizudb",
        user="aroy",
        password="zBkTCasbbGJiuZfESJcr"
    )
    cursor = conn.cursor()
    return conn, cursor


# Function to fetch all flight numbers from the bookings database
def fetch_all_flight_numbers():
    conn, cursor = get_db_cursor()
    try:
        select_query = "SELECT flight_number FROM bookings;"
        cursor.execute(select_query)
        flight_numbers = cursor.fetchall()
        return [flight[0] for flight in flight_numbers]  # Return a list of flight numbers
    finally:
        cursor.close()
        conn.close()


# Function to fetch current flight status from the database
def fetch_flight_status(flight_number):
    conn, cursor = get_db_cursor()
    try:
        select_query = sql.SQL("SELECT status FROM bookings WHERE flight_number = %s;")
        cursor.execute(select_query, (flight_number,))
        flight_status = cursor.fetchone()
        if flight_status:
            return flight_status[0]  # Return the status if found
        else:
            return None  # No booking found
    finally:
        cursor.close()
        conn.close()


# Function to fetch booking ID for a given flight number
def fetch_booking_id(flight_number):
    conn, cursor = get_db_cursor()
    try:
        select_query = sql.SQL("SELECT id FROM bookings WHERE flight_number = %s;")
        cursor.execute(select_query, (flight_number,))
        booking_id = cursor.fetchone()
        if booking_id:
            return booking_id[0]  # Return the booking_id if found
        else:
            return None  # No booking found
    finally:
        cursor.close()
        conn.close()


# Function to update booking status in case of disruption
def update_booking_status(flight_number, new_status, reason):
    conn, cursor = get_db_cursor()
    try:
        update_query = sql.SQL("""
            UPDATE bookings 
            SET status = %s, disrupted = %s, disruption_reason = %s 
            WHERE flight_number = %s;
        """)
        cursor.execute(update_query, (new_status, True, reason, flight_number))
        conn.commit()
        print(f"Booking updated for flight {flight_number}: {new_status}")

        # Notify customer (placeholder function)
        notify_customer(flight_number, new_status)
    finally:
        cursor.close()
        conn.close()


def create_disruption_logs_table():
    """Create the disruption_logs table if it does not exist."""
    conn, cursor = get_db_cursor()
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS disruption_logs (
                flight_number VARCHAR(50),
                status VARCHAR(20),
                disruption_reason TEXT,
                timestamp TIMESTAMP,
                user_decision TEXT
            );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("disruption_logs table created or already exists.")
    finally:
        cursor.close()
        conn.close()


def insert_disruption_log(flight_number, status, reason, user_decision):
    conn, cursor = get_db_cursor()
    try:
        insert_query = sql.SQL("""
            INSERT INTO disruption_logs (flight_number, status, disruption_reason, timestamp, user_decision)
            VALUES (%s, %s, %s, %s, %s);
        """)
        cursor.execute(insert_query, (flight_number, status, reason, datetime.datetime.now(), user_decision))
        conn.commit()
    finally:
        cursor.close()
        conn.close()


# Function to notify customers
def notify_customer(flight_number, status):
    # Placeholder for sending notifications
    print(f"Customer notified: Flight {flight_number} is now {status}.")


# Function to check for flight disruptions using the current status
def handle_disruption(flight_number):
    current_status = fetch_flight_status(flight_number)

    if current_status is None:
        print(f"No booking found for flight {flight_number}.")
        return

    print(f"Current status for flight {flight_number}: {current_status}")

    if current_status in ["Delayed", "Cancelled"]:
        disruption_reason = f"Flight status changed to {current_status}"
        update_booking_status(flight_number, current_status, disruption_reason)

        # Fetch booking id for the flight
        booking_id = fetch_booking_id(flight_number)

        if booking_id:
            # Offer the user a choice to either withdraw or reschedule
            offer_user_choice(flight_number, booking_id)
        else:
            print(f"No booking ID found for flight {flight_number}.")
    else:
        print("No disruptions detected.")


# Function to withdraw the ticket amount
def withdraw_ticket(flight_number, booking_id):
    conn, cursor = get_db_cursor()
    try:
        # Mark the booking as refunded
        update_query = sql.SQL("""
            UPDATE bookings 
            SET is_refunded = %s, status = %s 
            WHERE flight_number = %s AND id = %s;
        """)
        cursor.execute(update_query, (True, "Refunded", flight_number, booking_id))
        conn.commit()
        print(f"Ticket withdrawn for flight {flight_number}, booking {booking_id}.")
        notify_customer(flight_number, "Your ticket has been refunded.")

        # Insert log into disruption_logs
        insert_disruption_log(flight_number, "Refunded", "User withdrew ticket.", "Withdrawn")
    finally:
        cursor.close()
        conn.close()


# Function to reschedule the ticket to the next available flight
def reschedule_ticket(flight_number, booking_id, new_flight_number):
    conn, cursor = get_db_cursor()
    try:
        # Mark the booking as rescheduled
        update_query = sql.SQL("""
            UPDATE bookings 
            SET is_rescheduled = %s, reschedule_flight_number = %s, status = %s 
            WHERE flight_number = %s AND id = %s;
        """)
        cursor.execute(update_query, (True, new_flight_number, "Rescheduled", flight_number, booking_id))
        conn.commit()
        print(f"Ticket rescheduled from flight {flight_number} to {new_flight_number}, booking {booking_id}.")
        notify_customer(flight_number, f"Your ticket has been rescheduled to flight {new_flight_number}.")

        # Insert log into disruption_logs
        insert_disruption_log(flight_number, "Rescheduled", "User rescheduled ticket.", "Rescheduled to " + new_flight_number)
    finally:
        cursor.close()
        conn.close()


# Function to handle user choice (withdraw or reschedule)
def offer_user_choice(flight_number, booking_id):
    # Simulate user interaction (in real system, this would be a user interface interaction)
    print(f"Flight {flight_number} is disrupted. What would you like to do?")
    print("1. Withdraw ticket and get a refund")
    print("2. Reschedule to the next available flight")

    choice = input("Enter your choice (1 or 2): ")

    if choice == "1":
        withdraw_ticket(flight_number, booking_id)
    elif choice == "2":
        # For simplicity, we'll assume the next flight is available; you can expand this logic
        new_flight_number = get_next_available_flight(flight_number)
        if new_flight_number:
            reschedule_ticket(flight_number, booking_id, new_flight_number)
        else:
            print("No available flights to reschedule.")
    else:
        print("Invalid choice. Please choose 1 or 2.")


# Simulate fetching the next available flight (this function can be expanded based on your data)
def get_next_available_flight(current_flight):
    # Placeholder for logic to get next available flight
    return "NEW123"  # Example next available flight


# Main loop to process all flights in the database
if _name_ == "_main_":
    # Ensure the disruption_logs table is created
    create_disruption_logs_table()

    # Fetch and process flight disruptions
    flight_numbers = fetch_all_flight_numbers()
    print(f"Found {len(flight_numbers)} flight numbers in the database.")

    for flight_number in flight_numbers:
        print(f"Checking disruption for flight: {flight_number}")
        handle_disruption(flight_number)

    print("All flight disruptions processed.")