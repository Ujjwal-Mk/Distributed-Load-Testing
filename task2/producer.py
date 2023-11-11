from kafka import KafkaProducer
import sys


topics=sys.argv[1:]

producer=KafkaProducer(
    bootstrap_servers="localhost:9092"
)
try:
    for line in sys.stdin:
        producer.send(sys.argv[1],value=line.encode("utf-8"))
        producer.send(sys.argv[2],value=line.encode("utf-8"))
        producer.send(sys.argv[3],value=line.encode("utf-8"))

    # for topic in topics:
    #     producer.send(topic, value="EOF".encode('utf-8'))

    producer.close()
except:
    producer.close()