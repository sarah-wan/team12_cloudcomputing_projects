
import sys
from pyspark.sql import SparkSession
import couchdb
import pandas as pd
import numpy as np

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

    for i in range(10):
        counts = df.map(lambda x: (x["house_id"] + "," x["household_id"] + "," x["plug_id"], np.array[x["value"], 1, 0, 0]) if (x["property"] == 0) else (x["house_id"] + "," x["household_id"] + "," x["plug_id"], np.array[0, 0, x["value"], 1])).reduceByKey(lambda a,b: np.add(a,b)).
        output = counts.collect()

    outdb = "solution"
    if solution in couchserver:
        db = couchserver[outdb]
    else:
        db = couchserver.create(outdb)
    
    for (msg : output)
        print(msg)



