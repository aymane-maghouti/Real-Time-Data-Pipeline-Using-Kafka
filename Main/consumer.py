from kafka import KafkaConsumer
import pyodbc
import json
from datetime import datetime


def consum():
    # Kafka broker configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'firstTopic'

    # Create a Kafka consumer
    consumer = KafkaConsumer(topic,
                             group_id='my_consumer_group',
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: x.decode('utf-8'))

    try:
        # Establish a connection to the SQL Server database
        cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};"
                    "Server=DESKTOP-4KMK01F\\SQLEXPRESS;"
                    "Database=System_Performance;"
                    "Trusted_Connection=yes;")
        sql_connection = pyodbc.connect(cnxn_str)
        sql_cursor = sql_connection.cursor()

        for message in consumer:
            try:
                # Split the message value into individual values
                values = message.value.split(',')
                # Unpack the values into variables
                current_datetime, cpu_usage, memory_usage, cpu_interrupts, cpu_calls, memory_used, memory_free, bytes_sent, bytes_received, disk_usage = values

                # Extract datetime without milliseconds
                formatted_datetime = current_datetime.split('.')[0]

                # Format current_datetime to the correct format for SQL Server
                formatted_datetime = datetime.strptime(formatted_datetime, '%Y-%m-%d %H:%M:%S')

                # Insert data into SQL Server database
                insert_query = "INSERT INTO Performance (time, cpu_usage, memory_usage, cpu_interrupts, cpu_calls, memory_used, memory_free, bytes_sent, bytes_received, disk_usage) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                data_to_insert = (
                formatted_datetime, cpu_usage, memory_usage, cpu_interrupts, cpu_calls, memory_used, memory_free,
                bytes_sent, bytes_received, disk_usage)
                sql_cursor.execute(insert_query, data_to_insert)
                sql_connection.commit()

                print(f"Inserted at {formatted_datetime} into the database.")
                print("-------------------")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue

    except KeyboardInterrupt:
        print("Consumer stopped.")

    finally:
        # Close connections
        sql_cursor.close()
        sql_connection.close()

        print("Consumer and database connections closed.")
