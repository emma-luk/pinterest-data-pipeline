import requests
import json
import random
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from db_utils import AWSDBConnector

# Define the base URL for the API endpoint
API_BASE_URL = "https://zs9bvz0s74.execute-api.us-east-1.amazonaws.com/API/streams/"

# Define the stream names for the Pinterest tables
pin_stream_name = "streaming-0af0031518e7-pin"
geo_stream_name = "streaming-0af0031518e7-geo"
user_stream_name = "streaming-0af0031518e7-user"

# Define the headers for the HTTP request
headers = {'Content-Type': 'application/json'}

# Function to fetch data from the database
def fetch_data_from_database(table_name):
    '''
    what does this function do
    
    What are the inputs
    
    What are the outputs
    '''
    connector = AWSDBConnector()
    engine = connector.create_db_connector()
    with engine.connect() as connection:
        query_string = text(f"SELECT * FROM {table_name} ORDER BY RAND() LIMIT 1")
        result = connection.execute(query_string)
        for row in result:
            return dict(row._mapping)

# Function to send data to Kinesis stream
def send_data_to_kinesis(stream_name, data):
    '''
    what does this function do
    
    What are the inputs
    
    What are the outputs
    '''
    # Convert datetime objects to strings
    data = convert_datetime_to_string(data)
    
    # Construct the full API URL for the specific stream
    invoke_url = f"{API_BASE_URL}{stream_name}/record"

    # Construct the payload
    payload = json.dumps({
        "StreamName": stream_name,
        "Data": data,
        "PartitionKey": "desired-name"
    })

    # Send the HTTP request
    response = requests.put(invoke_url, headers=headers, data=payload)
    
    # Print the response status code
    print(f"Response status code for {stream_name}: {response.status_code}")

# Function to convert datetime objects to strings
def convert_datetime_to_string(data):
    '''
    what does this function do
    
    What are the inputs
    
    What are the outputs
    '''
    converted_data = {}
    for key, value in data.items():
        if isinstance(value, datetime):
            converted_data[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            converted_data[key] = value
    return converted_data

if __name__ == "__main__":
    while True:
        # Fetch and send data to the Kinesis streams for the three Pinterest tables
        pin_data = fetch_data_from_database("pinterest_data")
        geo_data = fetch_data_from_database("geolocation_data")
        user_data = fetch_data_from_database("user_data")
        
        send_data_to_kinesis(pin_stream_name, pin_data)
        send_data_to_kinesis(geo_stream_name, geo_data)
        send_data_to_kinesis(user_stream_name, user_data)
