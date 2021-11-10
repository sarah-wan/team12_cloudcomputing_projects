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
# We can make this more sophisticated/elegant but for now it is just
# hardcoded to the setup I have on my local VMs

# acquire the consumer
# (you will need to change this to your bootstrap server's IP addr)
consumer = KafkaConsumer (bootstrap_servers=["129.114.25.146:30000, 129.114.25.146:30001, 129.114.25.146:30002"], api_version=(2,8,0), value_deserializer=lambda m: json.loads(m.decode('utf-8')))
#consumer = KafkaConsumer (bootstrap_servers=["34.207.182.122:9092", "54.144.52.166:9092"], api_version=(2,8,0), ,   value_deserializer=lambda m: json.loads(m.decode('utf-8'))))

# subscribe to topic
consumer.subscribe (topics=["utilizations", "beef", "chocolate", "chicken"])

#Set up database
couchserver = couchdb.Server('http://admin:team12@129.114.25.146:30006/')
#couchserver = couchdb.Server('https://admin:team12@54.144.52.166:5984/')
dbname = "kafka-consumer"

if dbname in couchserver:
    db = couchserver[dbname]
else:
    db = couchserver.create(dbname)

# we keep reading and printing
for msg in consumer:
    # what we get is a record. From this record, we are interested in printing
    # the contents of the value field. We are sure that we get only the
    # utilizations topic because that is the only topic we subscribed to.
    # Otherwise we will need to demultiplex the incoming data according to the
    # topic coming in.
    #
    # convert the value field into string (ASCII)
    #
    # Note that I am not showing code to obtain the incoming data as JSON
    # nor am I showing any code to connect to a backend database sink to
    # dump the incoming data. You will have to do that for the assignment.
    #print (str(msg.value, 'ascii'))
    db.save(msg.value)

# we are done. As such, we are not going to get here as the above loop
# is a forever loop.
consumer.close ()