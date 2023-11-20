from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
import uuid
import argparse

class Orchestrator:
    def __init__(self, kafka_server, orchestrator_ip):
        self.kafka_server = kafka_server
        self.orchestrator_ip=orchestrator_ip
        self.heartbeat_timeout=5

        self.last_heartbeat_timestamps={}

        # Configure Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        # Configure Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.kafka_server,
            group_id='orchestrator_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        self.consumer.subscribe(['register', 'trigger', 'metrics', 'test_config', 'heartbeat', 'end_test'])
        
        # Metrics store
        self.metrics_store = {}
        
        # Test store
        self.tests = {}

        # Register Orchestrator
        self.register(self.orchestrator_ip)

        # Start threads for consuming messages and updating metrics
        threading.Thread(target=self.consume_messages, daemon=True).start()
        threading.Thread(target=self.check_inactive_nodes, daemon=True).start()
        threading.Thread(target=self.send_beat, daemon=True).start()
        # threading.Thread(target=self.update_metrics_dashboard, daemon=True).start()

    def register(self, orchestrator_ip):
        # Orchestrator registers itself with a new node_id, node_IP, and message_type
        registration_message = {
            'node_id': str(uuid.uuid4())[:8],
            'node_IP': orchestrator_ip,
            'message_type': 'ORCHESTRATOR_NODE_REGISTER'
        }
        # self.producer.send('register', value=registration_message)
    
    def send_beat(self):
        self.producer.send("master-check",value="orchestrator available")

    def consume_messages(self):
        for message in self.consumer:
            topic = message.topic
            value = message.value
            if topic == 'register':
                self.handle_registration(value)
            elif topic == 'metrics':
                self.handle_metrics(value)
            elif topic == 'heartbeat':
                self.handle_heartbeat(value)
            elif topic=='end_test':
                print(f"{value}")

    def handle_registration(self, registration_message):
        # Handle registration messages from driver nodes
        node_id = registration_message['node_id']
        self.metrics_store[node_id] = []

    def handle_metrics(self, metrics_message):
        # Handle metrics messages received from driver nodes
        node_id = metrics_message['node_id']
        test_id = metrics_message["test_id"]
        key=node_id
        value=[metrics_message["test_id"],metrics_message["report_id"],metrics_message["metrics"]]
        if key not in self.metrics_store.keys():
            self.metrics_store[key]=[value]
        else:
            self.metrics_store[key].append(value)

    def handle_heartbeat(self, heartbeat_message):
        # Handle heartbeat message received from nodes
        node_id = heartbeat_message['node_id']
        self.last_heartbeat_timestamps[node_id] = time.time()
        # print(heartbeat_message)
    
    def check_inactive_nodes(self):
        while True:
            current_time = time.time()
            inactive_nodes = [node for node, last_heartbeat_time in self.last_heartbeat_timestamps.items() if current_time - last_heartbeat_time >= self.heartbeat_timeout]

            for node_id in inactive_nodes:
                self.metrics_store.pop(node_id, None)  # Use dict.pop(key, None) to avoid KeyError
                self.last_heartbeat_timestamps.pop(node_id, None)
                print(f"\nDriver Node {node_id} removed due to inactivity.")

            time.sleep(0.1)  # Adjust the sleep interval as needed

    def update_metrics_dashboard(self):
        # while True:
            # Print or display the metrics dashboard
        print("Metrics Dashboard:")
        for node_id, metrics in self.metrics_store.items():
            print(f"Driver Node {node_id}: {metrics}")
        print("-" * 50)
        # time.sleep(1)

    def create_test(self, test_type, test_delay):
        test_id = str(uuid.uuid4())
        send_message = {
            'test_id': test_id,
            'test_type': test_type,
            'test_message_delay': test_delay
        }
        self.tests[send_message["test_id"]]=[send_message['test_type'], send_message['test_message_delay']]
        self.producer.send('test_config', value=send_message)
        print(self.tests)
        return test_id

    def trigger_test(self, test_id):
        # Trigger a specific test by sending a message to the 'trigger' topic
        if test_id in self.tests.keys():
            send_message = {
                'test_id': test_id,
                'trigger': 'YES'
            }
            self.producer.send('trigger', value=send_message)
            return True
        else:
            print(f"Test with ID {test_id} not found.")
            return False

    def death(self):
        self.producer.send('end',value="KILL DRIVERS")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Orchestrator Node')
    parser.add_argument('--kafka-server', default='localhost:9092', help='Kafka server address')
    parser.add_argument('--orchestrator-ip', default=192168, help='Orchestrator IP')
    args = parser.parse_args()

    orchestrator = Orchestrator(args.kafka_server, args.orchestrator_ip)
    while True:
        print("1. Create Test")
        print("2. Trigger a Load Test")
        print("3. Display Metrics")
        print("4. Display Test Configuration(s)")
        print("5. Exit")
        choice = input("Enter your choice: ")
        if choice == '1':
            test_type = input("Enter test type (avalanche/tsunami): ")
            test_delay = float(input("Enter test delay (in seconds for tsunami, 0 for avalanche): "))
            test_id = orchestrator.create_test(test_type, test_delay)
            print(f"Test created with ID: {test_id}")
        elif choice == '2':
            test_id = input("Enter the test id you want to trigger: ")
            orchestrator.trigger_test(test_id)
        elif choice == '3':
            orchestrator.update_metrics_dashboard()
        elif choice == '4':
            print('\n',orchestrator.tests,'\n')
        elif choice == '5':
            orchestrator.death()
            time.sleep(1)
            break
        else:
            print("Invalid choice. Please try again.")