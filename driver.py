from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
import uuid
import socket
import requests
import argparse
import signal
import sys

TOTALREQUESTS = 1000
CHECKONLINECHANCE=1

class DriverNode:
    def __init__(self, kafka_server, server_url, throughput, exit_event, requests):
        # Generate node_id and node_IP
        self.TOTALREQUESTS = requests
        self.reserve = requests
        self.node_id = str(uuid.uuid4())[:8]
        self.node_IP = socket.gethostbyname(socket.gethostname())

        self.throughput = throughput if throughput else 1
        self.kafka_server = kafka_server
        self.server_url = server_url
        self.requests_sent = 0
        self.responses_received = 0
        self.exit_event = exit_event  # Event for signaling threads to exit

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

        # Create a Kafka consumer for master-check
        self.master_check_consumer = KafkaConsumer(
            bootstrap_servers=self.kafka_server,
            group_id=f"{self.node_id}-master-check",
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Start threads for consuming messages, sending heartbeat, and master check
        self.t1 = threading.Thread(target=self.consume_messages, daemon=True)
        self.t1.start()
        self.t2 = threading.Thread(target=self.heartbeat, daemon=True)
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
            if self.exit_event.is_set():
                break  # Exit the thread if the exit event is set
            topic = message.topic
            value = message.value
            if topic == 'test_config':
                self.handle_test_config(value)
            elif topic == 'trigger':
                self.handle_trigger(value)
            elif topic == 'end':
                self.end_thread(value)
            elif topic == 'master-check':
                self.orchestrator=True

    def end_thread(self, value):
        self.exit_event.set()  # Set the exit event to signal threads to exit

    def handle_test_config(self, test_config):
        test_id = test_config['test_id']
        test_type = test_config['test_type']
        test_message_delay = test_config['test_message_delay']

        print(f"Received Test Configuration - Test ID: {test_id}, Type: {test_type}, Delay: {test_message_delay}")

        self.test_configs.append(test_config)

    def handle_trigger(self, trigger_message):
        test_id = trigger_message['test_id']
        print(f"Received Trigger Message for Test ID: {test_id}")

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
            self.test_configs.remove(matching_test)
        else:
            print(f"No test configuration found for the received trigger (Test ID: {test_id}).")

    def avalanche(self, test_config):
        self.current_test_id = test_config['test_id']
        checks = 10
        delay = 1 / self.throughput
        while True and self.TOTALREQUESTS > 0:
            if self.exit_event.is_set():
                break
            time.sleep(delay)
            response_time = self.send_request_to_server()
            self.update_metrics(response_time)
            self.TOTALREQUESTS -= 1
        self.producer.send("end_test", value=f"{self.node_id} finished testing")
        self.send_metrics()
        self.TOTALREQUESTS = self.reserve

    def tsunami(self, test_config):
        self.current_test_id = test_config['test_id']
        time_delay = test_config['test_message_delay']
        checks = 10
        while True and self.TOTALREQUESTS > 0:
            if self.exit_event.is_set():
                break
            time.sleep(time_delay / self.throughput)
            response_time = self.send_request_to_server()
            self.update_metrics(response_time)
            self.TOTALREQUESTS -= 1
        self.producer.send("end_test", value=f"{self.node_id} finished testing")
        self.send_metrics()
        self.TOTALREQUESTS = self.reserve

    def send_metrics(self):
        metrics_message = {
            'node_id': self.node_id,
            'test_id': self.current_test_id,
            'report_id': str(uuid.uuid4())[:8],
            'metrics': {
                'mean_latency': sum(self.metrics['latencies']) / len(self.metrics['latencies']),
                'median_latency': self.calculate_median(self.metrics['latencies']),
                'min_latency': min(self.metrics['latencies']),
                'max_latency': max(self.metrics['latencies']),
                'requests_sent': self.requests_sent,
                'responses_received': self.responses_received
            }
        }
        self.producer.send('metrics', value=metrics_message)

    def heartbeat(self):
        while True:
            if self.exit_event.is_set():
                break
            heartbeat_message = {
                'node_id': self.node_id,
                'heartbeat': 'YES'
            }
            self.producer.send('heartbeat', value=heartbeat_message)
            time.sleep(1 / 8)

    def check_master_availability(self, orchestrator):
        global CHECKONLINECHANCE
        if orchestrator:
            pass
        else:
            time.sleep(5)
            if CHECKONLINECHANCE==0:
                print(f"No Orchestrator Node detected. Exiting driver nodes")
                self.exit_event.set()
            CHECKONLINECHANCE-=1

    def send_request_to_server(self):
        self.requests_sent += 1
        self.producer.send("ping", value=self.node_id)
        start_time = time.time()
        response = requests.get(self.server_url)
        end_time = time.time()
        response_time = end_time - start_time
        self.responses_received += 1
        print(response.status_code, self.node_id, self.TOTALREQUESTS)
        return response_time

    def update_metrics(self, response_time):
        self.metrics['latencies'].append(response_time)

    def calculate_median(self, data):
        sorted_data = sorted(data)
        n = len(sorted_data)
        if n % 2 == 0:
            median = (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
        else:
            median = sorted_data[n // 2]
        return median

def run_driver(kafka_server, server_url, throughput, exit_event, requests):
    driver_node = DriverNode(kafka_server, server_url, throughput, exit_event, requests)
    try:
        while not exit_event.is_set():
            pass
    except KeyboardInterrupt:
        exit_event.set()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Driver Node')
    parser.add_argument('--kafka-server', default='localhost:9092', help='Kafka server address')
    parser.add_argument('--server-url', default='http://localhost:9090/ping', help='Target server URL')
    parser.add_argument('--throughput', nargs='+', default=[1, 1], type=int, help='Per Node Throughput')
    parser.add_argument('--no-of-drivers', default=2, type=int, help="Number of driver nodes")

    args = parser.parse_args()
    n = args.no_of_drivers
    driverArr = []

    exit_event = threading.Event()

    # Create a Kafka consumer for orchestrator check
    orchestrator_check_consumer = KafkaConsumer(
        bootstrap_servers=args.kafka_server,
        group_id='orchestrator-check-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    orchestrator_check_consumer.subscribe(['master-check'])

    def signal_handler(sign, frame):
        print("\nStopping threads...")
        print("Requests left:", TOTALREQUESTS)
        exit_event.set()
        orchestrator_check_consumer.close()  # Close the orchestrator check consumer
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Check for orchestrator
    orchestrator_online = False
    for _ in range(2):  # Try checking for orchestrator for a limited number of attempts
        orchestrator_check_messages = orchestrator_check_consumer.poll(100)
        print(orchestrator_check_messages.messages)
        
        # Check if there are messages in the 'master-check' topic
        if 'master-check' in orchestrator_check_messages:
            orchestrator_status = orchestrator_check_messages['master-check'][0].value.get('status')
            if orchestrator_status == 'online':
                print("Orchestrator is online. Continuing...")
                orchestrator_online = True
                break
            elif orchestrator_status == 'offline':
                print("Orchestrator is offline. Looking for orchestrator...")
                time.sleep(5)
        else:
            print("No message received. Looking for orchestrator...")
            time.sleep(5)

    if not orchestrator_online:
        print("Orchestrator is not online. Exiting the program.")
        exit_event.set()
        orchestrator_check_consumer.close()
    
    elif orchestrator_online:
        try:
            for i in range(0, n):
                thread = threading.Thread(target=run_driver, args=(args.kafka_server, args.server_url, args.throughput[i], exit_event, TOTALREQUESTS // n))
                driverArr.append(thread)

            for i in range(0, n):
                driverArr[i].start()

            for i in range(0, n):
                driverArr[i].join()

        except KeyboardInterrupt:
            print('Perform KeyboardInterrupt one more time..')