#
#
# Author: Aniruddha Gokhale
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
# Created: Sept 6, 2020
#
# Purpose:
#
#    Demonstrate the use of Kafka Python streaming APIs.
#    In this example, demonstrate Kafka streaming API to build a consumer.
#

import os   # need this for popen
import time # for sleep
from kafka import KafkaConsumer  # consumer of events
import couchdb
import json
import requests
# We can make this more sophisticated/elegant but for now it is just
# hardcoded to the setup I have on my local VMs

# acquire the consumer
# (you will need to change this to your bootstrap server's IP addr)
consumer = KafkaConsumer (bootstrap_servers=["129.114.25.146:30000", "129.114.25.146:30001"], api_version=(2,8,0))
#consumer = KafkaConsumer (bootstrap_servers=["34.207.182.122:9092", "54.144.52.166:9092"], api_version=(2,8,0), ,   value_deserializer=lambda m: json.loads(m.decode('utf-8'))))

# subscribe to topic
consumer.subscribe (topics=["energy"])

#Set up database
couchserver = couchdb.Server('http://admin:team12@129.114.25.146:30006/')
#couchserver = couchdb.Server('https://admin:team12@54.144.52.166:5984/')
dbname = "kafka-consumer"

if dbname in couchserver:
    db = couchserver[dbname]
else:
    db = couchserver.create(dbname)

url= 'http://admin:team12@129.114.25.146:30006/kafka-consumer/_bulk_docs'
headers = {'content-type' : 'application/json'}

for msg in consumer:
    requests.post(url, data=msg.value, headers=headers)
# we are done. As such, we are not going to get here as the above loop
# is a forever loop.
consumer.close ()
