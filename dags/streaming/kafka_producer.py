

import json
import logging
import time
from kafka import KafkaProducer
from controllers.user_controller import UserController
from data import DataExtractor, DataTransformer


class DataStreamer:

    def __init__(self, bootstrap_servers, topic_name, duration=60):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, max_block_ms=5000)
        self.topic_name = topic_name
        self.duration = duration

    def stream_data(self):
        """Streams the provided data to the Kafka topic for a specified duration"""
        end_time = time.time() + self.duration

        while time.time() < end_time:
            
            # get data from the api
            extracted_data = DataExtractor().extract_data()           
            # transform it to json format
            transformed_data = DataTransformer().transform_data(extracted_data)
            
            if transformed_data:
                try:
                    data_str = json.dumps(transformed_data, indent=4)

                    self.producer.send(self.topic_name, data_str.encode("utf-8"))
                    
                    """ we igest data into our postgres table in this time to ensure persistence of all the data"""
                    UserController().insert_user(transformed_data)
                    
                    time.sleep(2)  # Adjust sleep duration as needed
                except Exception as e:
                    logging.error(f"Error streaming data: {e}")
                    continue  # Continue to the next iteration

        self.producer.close()  # Close the producer when done
