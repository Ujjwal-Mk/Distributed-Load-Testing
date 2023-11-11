#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import sys


tab=sys.argv
jsUsers={}
jslikes={}
jsshared={}
jscomments={}

def caluclate_popularity(jspop):
    for key in jsUsers:
        c,v,b=0,0,0
        if key in jslikes:
            c=jslikes[key]
        if key in jsshared:
            v=jsshared[key]
        if key in jscomments:
            b=jscomments[key]
        # print(key,c,v,b)
        jspop[key]=(c+(20*v)+(5*b))/1000
    return jspop

if len(tab)==4:
    consumer=KafkaConsumer(
        tab[3],
        bootstrap_servers='localhost:9092',
    )
    try:
        for message in consumer:
            data = message.value.decode('utf-8')
            if data.strip() == "EOF":
                break
            stuff=data.split()
            if stuff[2] not in jsUsers.keys():
                jsUsers[stuff[2]]=1
            if len(stuff)>=5 and stuff[0]=="comment":
                if stuff[2] not in jscomments.keys():
                    jscomments[stuff[2]]=1
                else:
                    jscomments[stuff[2]]+=1
            elif len(stuff)==4 and stuff[0]=="like":
                if stuff[2] not in jslikes.keys():
                    jslikes[stuff[2]]=1
                else:
                    jslikes[stuff[2]]+=1
            elif len(stuff)>=5 and stuff[0]=="share":
                if stuff[2] not in jsshared.keys():
                    jsshared[stuff[2]]=0
                jsshared[stuff[2]]+=len(stuff[4:])
        consumer.close()
        jspop={}
        caluclate_popularity(jspop)
        print(json.dumps(jspop, indent=4, sort_keys=True))
    except:
        consumer.close()
