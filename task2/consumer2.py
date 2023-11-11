#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import sys


consumer=KafkaConsumer(
    sys.argv[2],
    bootstrap_servers='localhost:9092'
)

jsLike={}
try:
    for message in consumer:
        data = message.value.decode('utf-8')
        # print(data)
        if data.strip() == "EOF":
            break
        stuff=data.split()
        # print(stuff)
        if len(stuff)==4 and "like"==stuff[0]:
            if stuff[2] not in jsLike:
                # print(stuff[2],"added")
                jsLike[stuff[2]]={}
                # print(stuff[3][:-2] not in jsLike[stuff[2]], stuff[2],stuff[3][:-2])
                # print(stuff[3], stuff[2])
            if stuff[3] not in jsLike[stuff[2]]:
                # print(stuff[2],stuff[3],"added")
                jsLike[stuff[2]][stuff[3]]=1
            else:
                # print(stuff[2],stuff[3],"inc")
                jsLike[stuff[2]][stuff[3]]+=1
    consumer.close()
    # print(json.dumps(jsLike,indent=4,sort_keys=True))
    sdi = dict(sorted(jsLike.items()))
    # print(sdi)
    print(json.dumps(sdi,indent=4))
except:
    consumer.close()
