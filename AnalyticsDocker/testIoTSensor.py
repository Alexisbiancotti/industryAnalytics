import random
import time
from kafka import KafkaProducer

# Kafka configuration
kafka_topic = 'testIoTSensor'
kafka_server = 'localhost:9092'  # Replace with your Kafka server address

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_server)

def simulate_sensor_data():
    # Simulate random sensor data (modify as needed)

    temperature = random.uniform(20, 70)  # Random temperature between 20 and 30

    return temperature

def send_data_to_kafka(temperature, machine):

    # Create a message in a format suitable for sending (e.g., a string or JSON)
    message = f'Temperature: {temperature}, Machine: {machine}'

    # Send the message to Kafka
    producer.send(kafka_topic, message.encode('utf-8'))

    print(f'Sent data to Kafka: {message}')


temp = simulate_sensor_data()

send_data_to_kafka(temp, "Iny1")

#revisar por que sin el flush no anda
producer.flush()