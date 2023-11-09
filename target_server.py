from kafka import KafkaProducer, KafkaConsumer
import json

producer = KafkaProducer(bootstrap_servers="localhost:9092")
consumer = KafkaConsumer('register_topic', 'endpoint_to_test_topic', 'metrics_topic', bootstrap_servers="localhost:9092")

def endpoint_to_test():
    print("Received request on the endpoint to test.")
    # Implement logic to handle the request and send a response
    # You can also record metrics and publish them to Kafka if needed

def get_metrics():
    print("Received request for metrics.")
    # Implement logic to provide metrics (requests sent, responses, etc.)

# Implement other functionalities, Kafka message handling, etc.

if __name__ == '__main__':
    # Subscribe to Kafka topics
    for message in consumer:
        data = json.loads(message.value)
        if message.topic == 'register_topic':
            print(f"Node registered: {data['node_id']} with IP: {data['node_IP']}")
        elif message.topic == 'endpoint_to_test_topic':
            endpoint_to_test()
        elif message.topic == 'metrics_topic':
            get_metrics()