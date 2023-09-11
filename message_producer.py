"""
JARROD SIMS
9/10/23

Message sender / emitter 

Description:
This script sends one message on a named queue.
It will execute and finish. 
You can change the message and run it again in the same terminal.

Remember:
- Use the up arrow to recall the last command executed in the terminal.
"""

# Import from Standard Library
import sys

# Import External packages used
import pika
import csv
import time

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)
rabbitmq_host = 'localhost'
queue_name = 'csv_stream'

# ---------------------------------------------------------------------------
# Define program functions (bits of reusable code)
# ---------------------------------------------------------------------------

# Read the CSV file and publish lines to RabbitMQ
def publish_line_to_rabbitmq(line, channel):
    channel.basic_publish(exchange='', routing_key=queue_name, body=line)

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue

    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))

        # use the connection to create a communication channel
        ch = conn.channel()

        # use the channel to declare a queue
        ch.queue_declare(queue=queue_name)

        # use the channel to publish a message to the queue
        #ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        with open(filename, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for line in csv_reader:
            line_str = ','.join(line)  # Convert the list to a CSV string
            publish_line_to_rabbitmq(line_str, channel)
            time.sleep(2)  # Wait for 2 seconds before publishing the next line
        # log a message for the user
        logger.info(f" [x] Sent {message}")

    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


# ---------------------------------------------------------------------------
# If this is the script we are running, then call some functions and execute code!
# ---------------------------------------------------------------------------
filename = "batchfile_3_farenheit.csv"

if __name__ == "__main__":
    send_message("localhost", "hello", "Hello World!")
