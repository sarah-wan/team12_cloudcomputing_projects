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
from urllib.parse import urlencode
import requests
import json
from bson import json_util

url_endpoint = "https://api.spoonacular.com/recipes/findByIngredients/?"


#s = input("Insert ingredients you want to use seperated by a comma: ")
#ingredients = s.split(",")
#number = input("Insert number of recipes you want to display: ")

request_parameter = {
    "apiKey": "fb67b807e0ee45a3930e3890b03b647c",
    "ingredients": "chocolate",
    "number": 5
}


#combine all the information together to access the api
recipes = requests.get(url_endpoint + urlencode(request_parameter)).json()

# We can make this more sophisticated/elegant but for now it is just
# hardcoded to the setup I have on my local VMs

# acquire the producer
# (you will need to change this to your bootstrap server's IP addr)
#producer = KafkaProducer (bootstrap_servers="129.114.25.146:9092", acks=1)  # wait for leader to write to log
producer = KafkaProducer (bootstrap_servers="129.114.25.146:30002", acks=1)

for recipe in recipes:

    
    recipe["time"] = time.time()
    # send the contents under topic utilizations. Note that it expects
    # the contents in bytes so we convert it to bytes.
    #
    # Note that here I am not serializing the contents into JSON or anything
    # as such but just taking the output as received and sending it as bytes
    # You will need to modify it to send a JSON structure, say something
    # like <timestamp, contents of top>
    #
    producer.send ("chocolate", json.dumps(recipe, default = json_util.default).encode('utf-8'))
    producer.flush ()   # try to empty the sending buffer

    # sleep a second
    time.sleep (1)

# we are done
producer.close ()
