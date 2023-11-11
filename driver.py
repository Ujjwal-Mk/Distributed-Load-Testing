from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
import time
import uuid

# producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], value_serializer = lambda x: dumps(x).encode("utf-8"))

#     send = {'EOF' : 1}
#        producer.send('topic name', value=send)

# consumer = KafkaConsumer('topic name', 
#                          bootstrap_servers=['localhost:9092'],
#                         #  auto_offset_reset='earliest',
#                         #  enable_auto_commit=True,
#                          value_deserializer=lambda x: loads(x.decode('utf-8')))

# object = object.value   -> object will become json object

def send_request(driver):
    pass


def calculate_metrics(driver):
    # use/modify driver.metrics
    pass

class Driver():
    def __init__(self, node_id, node_IP, message_type):
        self.intro = {
            'node_id':node_id,
            'node_IP':node_IP,
            'message_type':message_type
        }
        self.metrics={
            "mean_latency": "",
            "median_latency": "",
            "min_latency": "",
            "max_latency": "",
        }
    
    def send_metrics(self, test_id): #get the test id from which this driver was triggered
        report_id = None        # generated through some function
        sending_metrics={
            'node_id':self.node_id,
            'test_id':test_id,
            'report_id':report_id,
            'metrics':self.metrics
        }
        producer.send('metrics', sending_metrics)
    
    def heart(self):
        sending_heart={
            'node_id':self.node_id,
            'heartbeat':"YES"
        }
# implementing multiple drivers
driver_list=[]
for i in range(6):  # 6 should be a variable
    node_id = None # will have functions for these
    node_IP = None
    driver_list.append(Driver(node_id, node_IP, "DRIVER_NODE_REGISTER"))
    # producer.send('register_topic', value=driver_list[-1].intro)