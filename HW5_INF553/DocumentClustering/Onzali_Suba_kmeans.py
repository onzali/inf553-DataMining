#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
The K-means algorithm written from scratch against PySpark. In practice,
one may prefer to use the KMeans algorithm in ML, as shown in
examples/src/main/python/ml/kmeans_example.py.

This example requires NumPy (http://www.numpy.org/).
"""
from __future__ import print_function

import sys
from scipy import spatial
import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext
import math 

def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = spatial.distance.cosine(p,centers[i])
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Usage: kmeans <file> <k> <convergeDist>", file=sys.stderr)
        exit(-1)

    print("""WARN: This is a naive implementation of KMeans Clustering and is given
       as an example! Please refer to examples/src/main/python/ml/kmeans_example.py for an
       example on how to use ML's KMeans implementation.""", file=sys.stderr)

    spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()

    sc = spark.sparkContext

    f=open(sys.argv[1])
    number_of_documents=int(f.readline())
    number_of_Words=int(f.readline())
    vector_matrix=np.array([[0.0 for i in range(number_of_Words)] for j in range(number_of_documents)])
    number_of_words_in_one_doc=int(f.readline())
    with f:
        for line in f:
            doc,word,freq=line.split(" ")
            vector_matrix[int(doc)-1,int(word)-1]=float(freq)
    df_matrix=np.array([0.0 for j in range(number_of_Words)])
    for i in range(0,number_of_Words):
        df_matrix[i]=math.log(float(number_of_documents+1)/float(len(np.where(vector_matrix[:,i]!=0)[0])+1),2)

    for i in range(0,number_of_documents):
        vector_matrix[i,:]=np.multiply(vector_matrix[i,:],df_matrix)
        euclidean_distance=np.linalg.norm(vector_matrix[i,:])
        vector_matrix[i:]=np.divide(vector_matrix[i:],euclidean_distance)
    tfedPoints = sc.parallelize(vector_matrix).repartition(1)
    data = tfedPoints.cache()
    K = int(sys.argv[2])
    convergeDist = float(sys.argv[3])
    kPoints = data.takeSample(False, K, 1)
    tempDist = 1.0

    while tempDist > convergeDist:
        closest = data.map(
            lambda p: (closestPoint(p, kPoints), (p, 1)))
        pointStats = closest.reduceByKey(
            lambda p1_c1, p2_c2: (p1_c1[0] + p2_c2[0], p1_c1[1] + p2_c2[1]))
        newPoints = pointStats.map(
            lambda st: (st[0], st[1][0] / st[1][1])).collect()
        print(newPoints)
        tempDist = sum(np.sum((kPoints[iK] - p) ** 2) for (iK, p) in newPoints)

        for (iK, p) in newPoints:
            kPoints[iK] = p
    f = open(sys.argv[4],'w')
    for point in kPoints:
        f.write(str(len(np.where(point!=0.0)[0]))+"\n")
    spark.stop()


