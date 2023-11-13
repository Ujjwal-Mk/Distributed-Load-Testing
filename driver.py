from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
import uuid
import socket
import requests

class DriverNode:
    def __init__(self, kafka_server, server_url):
        # Generate node_id and node_IP
        self.node_id = str(uuid.uuid4())[:8]
        self.node_IP = socket.gethostbyname(socket.gethostname())

        self.kafka_server = kafka_server
        self.server_url = server_url

        # Configure Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        # Register the node
        self.register_node()

        # Configure Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.kafka_server,
            group_id=self.node_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Subscribe to relevant topics
        self.consumer.subscribe(['test_config', 'trigger'])

        # Array to store test configurations
        self.test_configs = []

        # Metrics
        self.metrics = {
            'latencies': [],  # Store latencies to calculate mean, median, min, max
        }

        # Current load test information
        self.current_test_id = None

        # Start threads for consuming messages and sending heartbeat
        threading.Thread(target=self.consume_messages, daemon=True).start()
        threading.Thread(target=self.heartbeat, daemon=True).start()

    def register_node(self):
        # Register the node by sending a registration message
        registration_message = {
            'node_id': self.node_id,
            'node_IP': self.node_IP,
            'message_type': 'DRIVER_NODE_REGISTER'
        }
        self.producer.send('register', value=registration_message)

    def consume_messages(self):
        for message in self.consumer:
            topic = message.topic
            value = message.value
            print(f"{self.node_id} received message")
            if topic == 'test_config':
                self.handle_test_config(value)
            elif topic == 'trigger':
                self.handle_trigger(value)

    def handle_test_config(self, test_config):
        # Handle test configuration received from Orchestrator
        test_id = test_config['test_id']
        test_type = test_config['test_type']
        test_message_delay = test_config['test_message_delay']

        print(f"Received Test Configuration - Test ID: {test_id}, Type: {test_type}, Delay: {test_message_delay}")

        # Store the test configuration for future reference
        self.test_configs.append(test_config)

    def handle_trigger(self, trigger_message):
        # Handle trigger message to start the load test
        test_id = trigger_message['test_id']
        print(f"Received Trigger Message for Test ID: {test_id}")

        # Find the matching test configuration for the received test ID
        matching_test = None
        for config in self.test_configs:
            if config['test_id'] == test_id:
                matching_test = config
                break

        if matching_test:
            if matching_test['test_type'] == 'avalanche':
                self.avalanche(matching_test)
            elif matching_test['test_type'] == 'tsunami':
                self.tsunami(matching_test)
            # Remove the started test configuration from the array
            self.test_configs.remove(matching_test)
        else:
            print(f"No test configuration found for the received trigger (Test ID: {test_id}).")

    def avalanche(self, test_config):
        # Implement Avalanche load testing logic
        self.current_test_id = test_config['test_id']
        checks = 10
        while True and checks>0:
            response_time = self.send_request_to_server()
            self.update_metrics(response_time)
            checks-=1
        self.send_metrics()

    def tsunami(self, test_config):
        # Implement Tsunami load testing logic
        self.current_test_id = test_config['test_id']
        time_delay = test_config['test_message_delay']
        checks = 10
        while True and checks>0:
            time.sleep(time_delay)
            response_time = self.send_request_to_server()
            self.update_metrics(response_time)
            checks-=1
        self.send_metrics()

    def send_metrics(self):
        # Send metrics to Orchestrator
        metrics_message = {
            'node_id': self.node_id,
            'test_id': self.current_test_id,
            'report_id': str(uuid.uuid4())[:8],
            'metrics': {
                'mean_latency': sum(self.metrics['latencies']) / len(self.metrics['latencies']),
                'median_latency': self.calculate_median(self.metrics['latencies']),
                'min_latency': min(self.metrics['latencies']),
                'max_latency': max(self.metrics['latencies']),
            }
        }
        self.producer.send('metrics', value=metrics_message)

    def heartbeat(self):
        while True:
            # Send heartbeat message to Orchestrator
            heartbeat_message = {
                'node_id': self.node_id,
                'heartbeat': 'YES'
            }
            self.producer.send('heartbeat', value=heartbeat_message)
            time.sleep(5)

    def send_request_to_server(self):
        # Simulate sending a request to the target server
        start_time = time.time()
        response = requests.get(self.server_url)
        end_time = time.time()
        response_time = end_time - start_time
        print(response.status_code)
        return response_time

    def update_metrics(self, response_time):
        # Update metrics with the received response time
        self.metrics['latencies'].append(response_time)

    def calculate_median(self, data):
        # Calculate the median of a list of numbers
        sorted_data = sorted(data)
        n = len(sorted_data)
        if n % 2 == 0:
            median = (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
        else:
            median = sorted_data[n // 2]
        return median

def run_driver(kafka_server, server_url):
    driver_node = DriverNode(kafka_server, server_url)
    while True:
        pass

if __name__ == '__main__':
    kafka_server = 'localhost:9092'
    server_url = 'https://www.amazon.in'

    n = int(input("Enter the number of driver nodes you want : "))
    driverArr=[]
    try:
        for i in range(0,n):
            thread = threading.Thread(target=run_driver, args=(kafka_server, server_url))
            driverArr.append(thread)
        
        for i in range(0,n):
            driverArr[i].start()
        
        for i in range(0,n):
            driverArr[i].join()
    except KeyboardInterrupt:
        print('Perform KeyboardInterrupt one more time..')