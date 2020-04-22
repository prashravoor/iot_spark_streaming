from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeansModel
from pyspark.sql.types import StructType
import numpy as np
import sys
import os
from pyspark.statcounter import StatCounter
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, window

EXPECTED_LABEL = 7
#EXPECTED_LABEL = 5
#EXPECTED_LABEL = 1
kmeanstrained = None

def printOp(taken):
    global kmeanstrained
    global EXPECTED_LABEL
    #taken = rdd.collect()
    vals = []
    for t in taken:
        try:
            vals.append(float(t[1]))
        except ValueError:
            vals.append(0.0)
    
    if len(vals) == 3:
        print('Batch processing in progress: {}'.format(vals))
        label = kmeanstrained.predict(np.array(vals))
        if not label == EXPECTED_LABEL:
            print('Current value: {}, labels: {}'.format(vals, label))
            takeAction()
    else:
        print('Some missing fields found: {}'.format(taken))
    print('Batch processing completed')

def takeAction():
    print()
    print('************ Action being taken *************')
    print()

def processBatch(df, wn):
    #df.show(20, False)
    vals = df.rdd.filter(lambda x: x['type'] in ['cpu', 'disk', 'net']) \
                 .map(lambda x: (x['type'], x['avg(value)'])) \
                 .combineByKey(lambda x: StatCounter([x]), StatCounter.merge, StatCounter.mergeStats) \
                 .mapValues(StatCounter.mean) \
                 .collect()
    printOp(vals)

if __name__ == '__main__':
    args = sys.argv
    
    if len(args) < 2:
        print('Usage: <cmd> <Streaming Directory> [<window size int>]')
        exit()

    modelpath = 'kmeans.trained'
    directories = args[1:]
    windowSize = 3
    if len(args) > 2:
        try:
           windowSize = int(args[-1])
           directories = directories[:-1]
        except ValueError:
            windowSize = 3

    spark = SparkSession \
        .builder \
        .appName("CPU Multi Structured Streaming") \
        .getOrCreate()

    sc = spark.sparkContext
    # load model
    if not os.path.exists(modelpath):
        print('KMeans model not found, train it first...')
        exit()
    else:
        print('Trained model found, loading...')
        clusters = KMeansModel.load(sc, modelpath)

    kmeanstrained = clusters

    userSchema = StructType().add('type', 'string').add('timestamp', 'timestamp').add('value', 'float')
    windowedDf = None
    streams = []
    for directory in directories:
        strm = spark.readStream.format('file').schema(userSchema).csv(directory)
        strm.groupBy(
                        'type',
                         window('timestamp', '{} seconds'.format(windowSize),
                                 '{} seconds'.format(max(0, windowSize-2)))
                    )
        streams.append(strm)

        if windowedDf is None:
            windowedDf = strm
        else:
            windowedDf = windowedDf.union(strm)

    print('Starting stream listener, window size: {}s'.format(windowSize))
    query = windowedDf.groupBy('type').avg().writeStream.outputMode('complete').format('console').foreachBatch(processBatch).start()
    query.awaitTermination()

