import json
from kafka import KafkaProducer, KafkaConsumer
import argparse
import time

def send_test_scenario(producer, topic, test_scenario):
    json_data = json.dumps(test_scenario).encode('utf-8')
    producer.send(topic, value=json_data)
    producer.flush()

def receive_test_results(consumer, topic):
    for msg in consumer:
        test_results = json.loads(msg.value.decode('utf-8'))
        print(f"Received test results from topic '{topic}': {test_results}")

def main(kafka_broker, load_test_scenarios_topic, test_results_topic):
    producer = KafkaProducer(bootstrap_servers=kafka_broker)
    consumer = KafkaConsumer(load_test_scenarios_topic, bootstrap_servers=kafka_broker, group_id='orchestrator_group')
    while True:
        test_scenario = {
            'test_type': 'avalanche',
            'target_website': 'https://www.amazon.in',
            'target_throughput': 100
        }
        send_test_scenario(producer, load_test_scenarios_topic, test_scenario)
        time.sleep(2)
        receive_test_results(consumer, test_results_topic)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Orchestrator Node for Load Testing')
    parser.add_argument('--kafka-broker', required=True, help='Kafka broker address (e.g., localhost:9092)')
    parser.add_argument('--load-test-scenarios-topic', required=True, help='Topic for load test scenarios')
    parser.add_argument('--test-results-topic', required=True, help='Topic for receiving test results')
    args = parser.parse_args()
    main(args.kafka_broker, args.load_test_scenarios_topic, args.test_results_topic)
