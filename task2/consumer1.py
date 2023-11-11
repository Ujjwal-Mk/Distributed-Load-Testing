#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import sys

oneConsumer=KafkaConsumer(
    sys.argv[1],
    bootstrap_servers='localhost:9092'
)

jsComments={}


def extract_comment(com):
	return " ".join(com)[1:-1]

for message in oneConsumer:
	data = message.value.decode('utf-8')
	if data.strip()=="EOF":
		break
	stuff = data.split()
	if len(stuff) >= 5 and stuff[0] == "comment":
    		toWhom = stuff[2]
    		post_id = stuff[3]
    		if toWhom not in jsComments:
    			jsComments[toWhom] = [extract_comment(stuff[4:])]
    		else:
    			jsComments[toWhom].append(extract_comment(stuff[4:]))
    			
print(json.dumps(jsComments,indent=4, sort_keys=True))
oneConsumer.close()