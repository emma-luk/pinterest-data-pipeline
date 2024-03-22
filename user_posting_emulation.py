import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from db_utils import AWSDBConnector

random.seed(100)


new_connector = AWSDBConnector()

# Define your API Invoke URL here
API_URL = "https://zs9bvz0s74.execute-api.us-east-1.amazonaws.com/API/topics/"

def transform_data(data):
    # Transform data here to remove datetime objects
    transformed_data = data.copy()  # Create a copy to avoid modifying the original data
    for key, value in transformed_data.items():
        if isinstance(value, datetime):
            transformed_data[key] = value.strftime('%Y-%m-%d %H:%M:%S')  # Convert datetime to string
    return transformed_data

def send_data_to_api(data, topic):
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    payload = json.dumps({
        "records": [
            {
                "value": transform_data(data)
            }
        ]
    })  
    url = f"{API_URL}{topic}"
    print(url)
    response = requests.post(url, headers=headers, data=payload, timeout=60)  
    print(f"Response from API for {topic}: {response.status_code}")
    return response.status_code == 200

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            # Send data to corresponding Kafka topics via API
            send_data_to_api(pin_result, "0af0031518e7.pin")
            send_data_to_api(geo_result, "0af0031518e7.geo")
            send_data_to_api(user_result, "0af0031518e7.user")

if __name__ == "__main__":
    run_infinite_post_data_loop()
