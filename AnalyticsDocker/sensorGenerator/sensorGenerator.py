import requests
import time
import random
import logging
from datetime import datetime

API_URL = "http://sensor-api:85/data"

def post_data():

    while True:

        parameters = {
                '1': {'moldTemp': 75, 'tempDev': 10, 'maq': 'Iny 1'},
                '2': {'moldTemp': 40, 'tempDev': 5, 'maq': 'Iny 2'},
                '3': {'moldTemp': 90, 'tempDev': 18, 'maq': 'Iny 3'},
            }
        
        for parameter in parameters:

            mean   = parameters[parameter]['moldTemp']
            stdDev = parameters[parameter]['tempDev']
            data = {
                    "createdAt": round(time.time()),
                    "mach": parameters[parameter]['maq'],
                    "temp": round(random.uniform(mean-stdDev, mean+stdDev),2)
                }
            try:
                response = requests.post(API_URL, json=data)
                if response.status_code == 200:
                    logging.info(f"Data posted successfully: {data}")
                else:
                    logging.warning(f"Failed to post data: {response.status_code} - {response.text}")

            except Exception as e:
                logging.warning(f"An error occurred: {e}")

        time.sleep(60)

if __name__ == "__main__":
    post_data()