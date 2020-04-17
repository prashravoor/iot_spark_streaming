from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.clustering import KMeansModel
import numpy as np
import sys
import os
from pyspark.statcounter import StatCounter

#EXPECTED_LABEL = 5
EXPECTED_LABEL = 1
kmeanstrained = None

def parse_line(line):
    val = line.split(',')

    if not len(val) == 2:
        print('Invalid value!: {}'.format(val))
        return []


    try:
        v = float(val[1])
    except ValueError:
        print('Invalid value.. {}'.format(val[1]))
        v = 0.0

    return [(val[0], v)]

def average(x, y):
    return (x+y) / 2.0

def printOp(rdd):
    global kmeanstrained
    global EXPECTED_LABEL
    taken = rdd.collect()
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

    sc = SparkContext("local[2]", "CpuUsageStreaming")
    ssc = StreamingContext(sc, windowSize)

    # load model
    if not os.path.exists(modelpath):
        print('KMeans model not found, train it first...')
        exit()
    else:
        print('Trained model found, loading...')
        clusters = KMeansModel.load(sc, modelpath)

    kmeanstrained = clusters
    streams = []
    for directory in directories:
        streams.append(ssc.textFileStream(directory))

    allstreams = ssc.union(*streams)

    #means = streams.flatMap(parse_line).combineByKey(lambda x: StatCounter([x]), StatCounter.merge, StatCounter.mergeStats).mapValues(StatCounter.mean)
    means = allstreams.flatMap(parse_line).combineByKey(lambda x: StatCounter([x]), StatCounter.merge, StatCounter.mergeStats).mapValues(StatCounter.mean)
    means.foreachRDD(printOp)

    print('Starting stream listener, window size: {}s'.format(windowSize))
    ssc.start()
    ssc.awaitTermination()

