from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
import uuid

class Orchestrator:
    def __init__(self, kafka_server):
        self.kafka_server = kafka_server

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

        self.consumer.subscribe(['register', 'trigger', 'metrics', 'test_config', 'heartbeat'])
        
        # Metrics store
        self.metrics_store = {}
        
        # Test store
        self.tests = {}

        # Register Orchestrator
        self.register()

        # Start threads for consuming messages and updating metrics
        threading.Thread(target=self.consume_messages, daemon=True).start()
        # threading.Thread(target=self.update_metrics_dashboard, daemon=True).start()

    def register(self):
        # Orchestrator registers itself with a new node_id, node_IP, and message_type
        registration_message = {
            'node_id': str(uuid.uuid4())[:8],
            'node_IP': 'localhost',
            'message_type': 'ORCHESTRATOR_NODE_REGISTER'
        }
        # self.producer.send('register', value=registration_message)

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

    def handle_registration(self, registration_message):
        # Handle registration messages from driver nodes
        node_id = registration_message['node_id']
        self.metrics_store[node_id] = {}

    def handle_metrics(self, metrics_message):
        # Handle metrics messages received from driver nodes
        node_id = metrics_message['node_id']
        self.metrics_store[node_id].update(metrics_message['metrics'])

    def handle_heartbeat(self, heartbeat_message):
        # Handle heartbeat message received from nodes
        node_id = heartbeat_message['node_id']
        # print(f"Heartbeat received from Node {node_id}")

    def update_metrics_dashboard(self):
        # while True:
            # Print or display the metrics dashboard
        print("Metrics Dashboard:")
        for node_id, metrics in self.metrics_store.items():
            print(f"Node {node_id}: {metrics}")
        print("-" * 20)
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
        return test_id

    def trigger_test(self, test_id):
        # Trigger a specific test by sending a message to the 'trigger' topic
        print(self.tests)
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

if __name__ == '__main__':
    kafka_server = 'localhost:9092'
    orchestrator = Orchestrator(kafka_server)
    while True:
        print("1. Create Test")
        print("2. Trigger a Load Test")
        print("3. Display Metrics")
        print("4. Exit")
        choice = input("Enter your choice: ")
        if choice == '1':
            test_type = input("Enter test type (avalanche/tsunami): ")
            test_delay = int(input("Enter test delay (in seconds for tsunami, 0 for avalanche): "))
            test_id = orchestrator.create_test(test_type, test_delay)
            print(f"Test created with ID: {test_id}")
        elif choice == '2':
            test_id = input("Enter the test id you want to trigger: ")
            orchestrator.trigger_test(test_id)
        elif choice == '3':
            orchestrator.update_metrics_dashboard()
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")