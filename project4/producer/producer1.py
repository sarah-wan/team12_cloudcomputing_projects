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
#    In this example, we use the "top" command and use it as producer of events for
#    Kafka. The consumer can be another Python program that reads and dumps the
#    information into a database OR just keeps displaying the incoming events on the
#    command line consumer (or consumers)
#

import os   # need this for popen
import time # for sleep
from kafka import KafkaProducer  # producer of events
import requests
import json
from bson import json_util
import path

total_rows = 1000000
num_producers = 2

# We can make this more sophisticated/elegant but for now it is just
# hardcoded to the setup I have on my local VMs

# acquire the producer
# (you will need to change this to your bootstrap server's IP addr)
#producer = KafkaProducer (bootstrap_servers="129.114.25.146:9092", acks=1)  # wait for leader to write to log
producer = KafkaProducer (bootstrap_servers="129.114.25.146:30000", acks=1)

fieldnames = ("id","timestamp","value", "property", "plug_id", "household_id", "house_id")
entries = []

with open('../energy.csv') as file:
    reader=csv.DictReader(file)
    nextrows=[row for idx, row in enumerate(reader) if idx in (0,100)]

    for row in nextrows:
        entry = OrderedDict()
        for field in fieldnames:
            entry[field] = row[field]
        entries.append(entry)
    
output = {
    "docs": entries
}
    
producer.send ("chicken", json.dumps(output, default = json_util.default).encode('utf-8'))
producer.flush ()   # try to empty the sending buffer

# we are done
producer.close ()
