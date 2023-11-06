import json
import time
from kafka import KafkaConsumer, KafkaProducer

class MetricsStore:
    def __init__(self):
        self.metrics = {}

    def get_metrics(self, test_id):
        return self.metrics.get(test_id, {})

class RuntimeController:
    def __init__(self, consumer, producer, metrics_store):
        self.consumer = consumer
        self.producer = producer
        self.metrics_store = metrics_store

        self.running_tests = {}

    def start(self):
        for message in self.consumer:
            if message.topic == 'test_config':
                test_config = json.loads(message.value.decode('utf-8'))

                # Start the load test.
                self.running_tests[test_config['test_id']] = Driver(test_config, self.producer, self.metrics_store)
                self.running_tests[test_config['test_id']].start()

            elif message.topic == 'trigger':
                test_id = json.loads(message.value.decode('utf-8'))['test_id']

                # Trigger the load test.
                if test_id in self.running_tests:
                    self.running_tests[test_id].trigger()

    def stop(self):
        for test_id, driver in self.running_tests.items():
            driver.stop()

class Orchestrator:
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            group_id='orchestrator'
        )
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092'
        )
        self.metrics_store = MetricsStore()

        self.runtime_controller = RuntimeController(self.consumer, self.producer, self.metrics_store)

    def start(self):
        self.runtime_controller.start()

    def stop(self):
        self.runtime_controller.stop()

    def trigger_load_test(self, test_id):
        self.producer.send('trigger', json.dumps({'test_id': test_id}).encode('utf-8'))

    def get_metrics(self, test_id):
        return self.metrics_store.get_metrics(test_id)

class Driver:
    def __init__(self, test_config, producer, metrics_store):
        self.test_config = test_config
        self.producer = producer
        self.metrics_store = metrics_store

        self.running = False

        self.metrics = {
            'mean_latency': 0,
            'median_latency': 0,
            'min_latency': 0,
            'max_latency': 0
        }

    def start(self):
        self.running = True

        # Send requests to the target server.
        for i in range(self.test_config['request_count']):
            start_time = time.time()
            response = self.send_request(self.test_config['target_server_url'])
            end_time = time.time()

            # Update the metrics.
            latency = end_time - start_time
            self.metrics['mean_latency'] = (self.metrics['mean_latency'] * i + latency) / (i + 1)
            if latency < self.metrics['min_latency']:
                self.metrics['min_latency'] = latency
            if latency > self.metrics['max_latency']:
                self.metrics['max_latency'] = latency

            # Send the metrics to the orchestrator.
            metrics_message = {
                'node_id': self.node_id,
                'test_id': self.test_config['test_id'],
                'metrics': self.metrics
            }
            self.producer.send('metrics', json.dumps(metrics_message).encode('utf-8'))

        # Wait for the load test to finish.
        time.sleep(self.test_config['duration'])

        self.running = False

    def stop(self):
        self.running = False

    def send_request(self, target_server_url):
        # TODO: Implement this method to send a request to the target server and return the
if __name__ == '__main__':
    # Example usage:
    test_config = {
        'test_id': 1,
        'target_server_url': 'http://localhost:8080',
        'request_count': 100,
        'duration': 10
    }
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    metrics_store = MetricsStore()
    driver = Driver(test_config, producer, metrics_store)
    driver.start()
    # Wait for the load test to finish.
    while driver.running:
        time.sleep(1)
    # Get the metrics for the load test.
    metrics = driver.get_metrics()
    # Print the metrics.
    print(metrics)