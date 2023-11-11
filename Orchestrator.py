from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
import uuid

producer = KafkaProducer(bootstrap_servers='your_kafka_server', value_serializer = lambda x: dumps(x).encode("utf-8"))

# heartbeats = KafkaConsumer('heartbeat','metrics'
#                          group_id='orchestrator_group',
#                          bootstrap_servers='your_kafka_server',
#                          value_deserializer=lambda x: loads(x.decode('utf-8')))

# metrics_consumer = KafkaConsumer('metrics',
#                          group_id='orchestrator_group',
#                          bootstrap_servers='your_kafka_server',
#                          value_deserializer=lambda x: loads(x.decode('utf-8')))

requests_sent = {}

class Orchestrator():
    def __init__(self, node_id, node_IP, message_type):
        self.intro = {
            'node_id':node_id,
            'node_IP':node_IP,
            'message_type':message_type
        }

#   needs to periodically check for heartbeat

    


def control_test():
    print("Received control request.")
    # Implement logic to control and trigger load tests
    # Publish messages to Kafka for configuration and triggering

def get_statistics(test_id):
    print(f"Fetching statistics for test {test_id}.")
    # Implement logic to fetch and display statistics for a given test

# Implement Runtime Controller, Kafka message handling, etc.
id = None   # to be generated
IP = None

O_Node = Orchestrator(id, IP, "ORCHESTRATOR_NODE_REGISTER")
producer.send('register', value=O_Node.intro)

# every time a user starts a test, a new test id and test_config_topic message needs to be created before the trigger_topic message 
# is sent

def create_test(test_type, test_delay):
    test_id = None      # to be generated
    send = {
        'test_id': test_id,
        'test_type': test_type,
        'test_message_delay': test_delay
    }
    producer.send('test_config', value=send)

# {
#   "test_id": <RANDOMLY GENERATED UNQUE TEST ID>,
#   "trigger": "YES",
# }

    return 

def start_test(test_id):

    producer.send('trigger', value={'test_id': test_id, 'trigger': 'YES'})

    return


for message in heartbeats:


    value = 0 



# {
#   "test_id": <RANDOMLY GENERATED UNQUE TEST ID>,
#   "test_type": "<AVALANCHE|TSUNAMI>",
#   "test_message_delay": "<0 | CUSTOM_DELAY>",
# }

# if __name__ == '__main__':
#     # Subscribe to Kafka topics
#     for message in consumer:
#         data = json.loads(message.value)
#         if message.topic == 'register_topic':
#             print(f"Node registered: {data['node_id']} with IP: {data['node_IP']}")
#         elif message.topic == 'control_topic':
#             control_test()
#         elif message.topic == 'statistics_topic':
#             get_statistics(data['test_id'])
