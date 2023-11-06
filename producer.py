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

if __name__ == '__main__':
    orchestrator = Orchestrator()
    orchestrator.start()

    while True:
        # Respond to HTTP requests.
        pass
