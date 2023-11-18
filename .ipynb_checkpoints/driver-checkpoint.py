from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
import uuid
import socket
import requests
import argparse

class DriverNode:
    def __init__(self, kafka_server, server_url, throughput):
        # Generate node_id and node_IP
        self.run = True
        self.node_id = str(uuid.uuid4())[:8]
        self.node_IP = socket.gethostbyname(socket.gethostname())

        self.throughput=throughput if throughput else 1
        self.kafka_server=kafka_server
        self.server_url = server_url
        self.requests_sent = 0

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
        self.consumer.subscribe(['test_config', 'trigger', 'end'])

        # Array to store test configurations
        self.test_configs = []

        # Metrics
        self.metrics = {
            'latencies': [],  # Store latencies to calculate mean, median, min, max
        }

        # Current load test information
        self.current_test_id = None

        # Start threads for consuming messages and sending heartbeat
        self.t1=threading.Thread(target=self.consume_messages, daemon=True)
        self.t1.start()
        self.t2=threading.Thread(target=self.heartbeat, daemon=True)
        self.t2.start()

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
            # print(f"{self.node_id} received message")
            if topic == 'test_config':
                self.handle_test_config(value)
            elif topic == 'trigger':
                self.handle_trigger(value)
            elif topic=='end':
                # print("a")
                self.end_thread(value)
    
    def end_thread(self, value):
        # print(value)
        # self.t1.join()
        # self.t2.join()
        # print(f"Driver {self.node_id} ended")
        self.run = False

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
        delay = 1/self.throughput
        while True and checks>0:
            time.sleep(delay)
            response_time = self.send_request_to_server()
            self.update_metrics(response_time)
            checks-=1
        self.producer.send("end_test",value=f"{self.node_id} finished testing")
        self.send_metrics()

    def tsunami(self, test_config):
        # Implement Tsunami load testing logic
        self.current_test_id = test_config['test_id']
        time_delay = test_config['test_message_delay']
        checks = 10
        while True and checks>0:
            time.sleep(time_delay/self.throughput)
            response_time = self.send_request_to_server()
            self.update_metrics(response_time)
            checks-=1
        self.producer.send("end_test",value=f"{self.node_id} finished testing")
        self.send_metrics()

    def send_metrics(self):
        # Send metrics to Orchestrator, including the requests_sent count
        metrics_message = {
            'node_id': self.node_id,
            'test_id': self.current_test_id,
            'report_id': str(uuid.uuid4())[:8],
            'metrics': {
                'mean_latency': sum(self.metrics['latencies']) / len(self.metrics['latencies']),
                'median_latency': self.calculate_median(self.metrics['latencies']),
                'min_latency': min(self.metrics['latencies']),
                'max_latency': max(self.metrics['latencies']),
                'requests_sent': self.requests_sent
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
            time.sleep(1/8)

    def send_request_to_server(self):
        # Simulate sending a request to the target server
        start_time = time.time()
        response = requests.get(self.server_url)
        end_time = time.time()
        response_time = end_time - start_time
        self.requests_sent += 1
        print(response.status_code, self.node_id)
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

def run_driver(kafka_server, server_url, throughput):
    driver_node = DriverNode(kafka_server, server_url, throughput)
    try:
        while not driver_node.run:
            pass
    except KeyboardInterrupt as e:
        print(driver_node.node_id,"ended")

if __name__ == '__main__':
    kafka_server = 'localhost:9092'
    server_url = 'http://localhost:9090'

    
    parser = argparse.ArgumentParser(description='Driver Node')
    parser.add_argument('--kafka-server', default='localhost:9092', help='Kafka server address')
    parser.add_argument('--server-url', default='http://localhost:9090', help='Target server URL')
    parser.add_argument('--no-of-drivers', default=1, type=int, help="Number of driver nodes")
    parser.add_argument('--throughput', nargs='+', default=[1], type=int, help='Per Node Throughput')

    args = parser.parse_args()
    n = args.no_of_drivers
    driverArr=[]
    try:
        for i in range(0,n):
            thread = threading.Thread(target=run_driver, args=(args.kafka_server, args.server_url, args.throughput[i]))
            driverArr.append(thread)

        for i in range(0,n):
            driverArr[i].start()

        for i in range(0,n):
            driverArr[i].join()
        
    except KeyboardInterrupt:
        print(' Perform KeyboardInterrupt one more time..')