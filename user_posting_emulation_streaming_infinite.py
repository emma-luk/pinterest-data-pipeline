import requests
import json
import random
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from db_utils import AWSDBConnector

class DataStreamer:
    """
    A class to fetch data from a database and stream it to Kinesis streams.

    Attributes:
    API_BASE_URL (str): The base URL for the API endpoint.
    pin_stream_name (str): The stream name for Pinterest data.
    geo_stream_name (str): The stream name for geolocation data.
    user_stream_name (str): The stream name for user data.
    headers (dict): Headers for the HTTP request.

    Methods:
    fetch_data_from_database(table_name): Fetches data from the database.
    send_data_to_kinesis(stream_name, data): Sends data to a Kinesis stream.
    convert_datetime_to_string(data): Converts datetime objects to strings.
    """

    # Define the base URL for the API endpoint
    API_BASE_URL = "https://zs9bvz0s74.execute-api.us-east-1.amazonaws.com/API/streams/"

    # Define the stream names for the Pinterest tables
    pin_stream_name = "streaming-0af0031518e7-pin"
    geo_stream_name = "streaming-0af0031518e7-geo"
    user_stream_name = "streaming-0af0031518e7-user"

    # Define the headers for the HTTP request
    headers = {'Content-Type': 'application/json'}

    def fetch_data_from_database(self, table_name):
        """
        Fetches data from the database.

        Parameters:
        table_name (str): The name of the table to fetch data from.

        Returns:
        dict: A dictionary containing fetched data.
        """
        connector = AWSDBConnector()
        engine = connector.create_db_connector()
        with engine.connect() as connection:
            query_string = text(f"SELECT * FROM {table_name} ORDER BY RAND() LIMIT 1")
            result = connection.execute(query_string)
            for row in result:
                return dict(row._mapping)

    def send_data_to_kinesis(self, stream_name, data):
        """
        Sends data to a Kinesis stream.

        Parameters:
        stream_name (str): The name of the Kinesis stream.
        data (dict): The data to be sent to the stream.
        """
        # Convert datetime objects to strings
        data = self.convert_datetime_to_string(data)
        
        # Construct the full API URL for the specific stream
        invoke_url = f"{self.API_BASE_URL}{stream_name}/record"

        # Construct the payload
        payload = json.dumps({
            "StreamName": stream_name,
            "Data": data,
            "PartitionKey": "desired-name"
        })

        # Send the HTTP request
        response = requests.put(invoke_url, headers=self.headers, data=payload)
        
        # Print the response status code
        print(f"Response status code for {stream_name}: {response.status_code}")

    def convert_datetime_to_string(self, data):
        """
        Converts datetime objects to strings.

        Parameters:
        data (dict): The data to be converted.

        Returns:
        dict: The data with datetime objects converted to strings.
        """
        converted_data = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                converted_data[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            else:
                converted_data[key] = value
        return converted_data

if __name__ == "__main__":
    data_streamer = DataStreamer()
    while True:
        # Fetch and send data to the Kinesis streams for the three Pinterest tables
        pin_data = data_streamer.fetch_data_from_database("pinterest_data")
        geo_data = data_streamer.fetch_data_from_database("geolocation_data")
        user_data = data_streamer.fetch_data_from_database("user_data")
        
        data_streamer.send_data_to_kinesis(data_streamer.pin_stream_name, pin_data)
        data_streamer.send_data_to_kinesis(data_streamer.geo_stream_name, geo_data)
        data_streamer.send_data_to_kinesis(data_streamer.user_stream_name, user_data)

