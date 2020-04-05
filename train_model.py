from pyspark import SparkContext
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
import numpy as np
import sys
import os

args = sys.argv
num_clusters = 10

if len(args) < 2:
    print('Usage: cmd <Training data file> [num clusters]')
    exit()
    

datafile = args[1]
if len(args) == 3:
    num_clusters = int(args[2])

modelpath = 'kmeans.trained'

if os.path.exists(modelpath):
    print('Dir exists')
    exit()

sc = SparkContext("local[2]", "CpuUsageStreaming")
# Load and parse the data
data = sc.textFile(datafile)
parsedData = data.map(lambda line: np.array([float(x) for x in line.split(',')]))

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, num_clusters, maxIterations=1000, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

cluster_sizes = parsedData.map(lambda e: clusters.predict(e)).countByValue()
print('Cluster sizes: {}'.format(cluster_sizes))

def printl(e):
    print(e, clusters.predict(e))

# Save model
clusters.save(sc, modelpath)

