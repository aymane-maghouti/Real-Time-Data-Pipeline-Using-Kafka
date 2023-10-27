import time
import psutil
import datetime
from producer import send_message
from consumer import consum
import threading

def producer_thread():
    while True:
        try:
            current_datetime = datetime.datetime.now()
            cpu_usage = psutil.cpu_percent()
            memory_usage = psutil.virtual_memory().percent
            cpu_interrupts = psutil.cpu_stats().interrupts
            cpu_calls = psutil.cpu_stats().syscalls
            memory_used = psutil.virtual_memory().used
            memory_free = psutil.virtual_memory().free
            bytes_sent = psutil.net_io_counters().bytes_sent
            bytes_received = psutil.net_io_counters().bytes_recv
            disk_usage = psutil.disk_usage('/').percent

            # Produce data to Kafka topic
            message = f"{current_datetime}, {cpu_usage}, {memory_usage}, {cpu_interrupts},{cpu_calls},{memory_used},{memory_free},{bytes_sent},{bytes_received},{disk_usage}"

            send_message(message)
            print("Message sent to Kafka topic")

            # Sleep for 5 seconds before collecting and sending the next set of data
            time.sleep(0.5)

        except Exception as e:
            print(f"Error in producer_thread: {str(e)}")

def consumer_thread():
    while True:
        try:
            consum()
            # Sleep for a short interval before consuming the next message
            time.sleep(0.1)
        except Exception as e:
            print(f"Error in consumer_thread: {str(e)}")

# Create separate threads for producer and consumer
producer_thread = threading.Thread(target=producer_thread)
consumer_thread = threading.Thread(target=consumer_thread)

# Start the threads
producer_thread.start()
consumer_thread.start()

# Wait for the threads to finish (which will never happen in this case as they run infinitely)
producer_thread.join()
consumer_thread.join()

