

import json
import logging
import time
from kafka import KafkaProducer

class DataStreamer:

    def __init__(self, bootstrap_servers, topic_name, duration=60):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, max_block_ms=5000)
        self.topic_name = topic_name
        self.duration = duration

    def stream_data(self, transformed_data):
        """Streams the provided data to the Kafka topic for a specified duration"""
        end_time = time.time() + self.duration

        while time.time() < end_time:
            try:
                data_str = json.dumps(transformed_data, indent=4)
                print(data_str)  # Or log this for better monitoring

                self.producer.send(self.topic_name, data_str.encode("utf-8"))

                time.sleep(2)  # Adjust sleep duration as needed
            except Exception as e:
                logging.error(f"Error streaming data: {e}")
                # Consider adding retry logic or other error handling here
                continue  # Continue to the next iteration

        self.producer.close()  # Close the producer when done
