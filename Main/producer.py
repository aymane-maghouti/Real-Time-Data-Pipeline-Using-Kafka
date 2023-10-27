from kafka import KafkaProducer

bootstrap_servers = 'localhost:9092'
topic = 'firstTopic'

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_message(message):
    try:
        producer.send(topic, value=message.encode('utf-8'))
        print(f"Produced: {message} to Kafka topic: {topic}")
    except Exception as error:
        print(f"Error: {error}")

