from kafka import KafkaConsumer

# Kafka configuration
kafka_topic = 'testIoTSensor'
kafka_server = 'localhost:9092'  # Replace with your Kafka server address

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_server,
    auto_offset_reset='earliest',  # Start from the earliest message
    group_id='my-group'            # Consumer group ID
)

print(f"Listening for messages on topic '{kafka_topic}'...")

# Continuously listen for messages
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")