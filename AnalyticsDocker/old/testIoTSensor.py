import random
import time
import json
from kafka import KafkaProducer

# Kafka configuration
kafka_topic = 'testIoTSensor'
kafka_server = 'localhost:9092'  # Replace with your Kafka server address

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_server,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8')) #nueva linea

def simulate_sensor_data():
    # Simulate random sensor data (modify as needed)

    temperature = random.uniform(20, 70)  # Random temperature between 20 and 30

    return round(temperature,2)

def send_data_to_kafka(temperature, machine):

    # Create a message in a format suitable for sending (e.g., a string or JSON)
    message = {'createdAt': round(time.time()),
                'mach': machine,
                'temp': temperature}

    # Send the message to Kafka
    producer.send(kafka_topic, value=message)

    print(f'Sent data to Kafka: {message}')


temp = simulate_sensor_data()

send_data_to_kafka(temp, "Iny1")

#revisar por que sin el flush no anda
producer.flush()