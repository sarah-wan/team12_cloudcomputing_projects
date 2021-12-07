
import sys
from pyspark.sql import SparkSession
import couchdb
import pandas as pd
import numpy as np
import time
import requests
import json

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    #Set up database
    couchserver = couchdb.Server('http://admin:team12@129.114.25.146:30006/')
    dbname = "kafka-consumer"

    #create pandas dataframe
    rows = couchserver[dbname].view('_all_docs', include_docs=True)
    data = [row['doc'] for row in rows]
    pandas_df = pd.DataFrame(data)

    df = spark.createDataFrame(pandas_df)

    output = 0

    f = open("timer-log.txt", "a")
    for i in range(10):
        start = time.time()
        seqOp = (lambda x, y: (x[0] + y[0], x[1]+1, x[2], x[3]) if (y[1] == 0) else (x[0], x[1], x[2] + y[0], x[3] + 1))
        combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2],  x[3] + y[3]))
        counts = df.rdd.map(lambda x : ((str(x["house_id"]) + "," + str(x["household_id"]) + "," + str(x["plug_id"])), (x["value"], x["property"]))).aggregateByKey((0,0,0,0), seqOp, combOp)
        #counts = df.rdd.map(lambda x: (x["house_id"] + "," + x["household_id"] + "," + x["plug_id"], (float(x["value"]), 1, 0, 0)) if (x["property"] == 0) else (x["house_id"] + "," + x["household_id"] + "," + x["plug_id"], (0, 0, x["value"], 1))).aggregateByKey((0,0,0,0), lambda)

        output = counts.collect()
        end = time.time()
        f.write(str(i) + ": " + str(end - start) + "\n")

    outdb = "solution"
    if outdb in couchserver:
        db = couchserver[outdb]
    else:
        db = couchserver.create(outdb)

    outdf = pd.DataFrame(index=np.arange(0, len(output)), columns = ('house_id', 'household_id', 'plug_id','avg_work' , 'avg_load'))

    x = 0
    for (key, value) in output:
        avg_work = 0
        avg_load = 0
        splitkey = key.split(",")
        if value[1] != 0:
            avg_work = value[0] / value[1]
        if value[3] != 0:
            avg_work = value[2] / value[3]
        outdf.loc[x] = [splitkey[0], splitkey[1], splitkey[2], avg_work, avg_load]
        x = x+1

    url = 'http://admin:team12@129.114.25.146:30006/solution/_bulk_docs'
    headers = {'content-type' : 'application/json'}

    myDocs = outdf.to_dict(orient='records')

    print(json.dumps({'docs':myDocs}))
    requests.post(url, data=json.dumps({'docs': myDocs}), headers=headers)
    #for (tot-work, work-count, tot-load, load-count) in output:
