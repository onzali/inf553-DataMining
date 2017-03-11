from collections import defaultdict
from pyspark import SparkContext
from collections import defaultdict
import sys
import itertools

def main():
    try:
        inputFile=sys.argv[1]
        outputFile=sys.argv[2]
        sc = SparkContext()
        data=sc.textFile(inputFile)
        userSetRdd=data.map(lambda line:[str(item) for item in line.split(',')]).map(lambda s: (int(s[0].split('U')[1]),s[1:]))
        movieSet=userSetRdd.flatMap(lambda (user,movies):[(int(movie),user) for movie in movies ]).groupByKey().mapValues(lambda values: [v for v in values ])
        signatures=[movieSet.flatMap(lambda s:computeSignatures(s,i)).reduceByKey(lambda x,y: x if x<y else y).sortByKey().map(lambda (x,y): y).collect() for i in xrange(0,20)]
        bands=sc.parallelize(signatures,5)
        candidatePairs=bands.mapPartitions(lambda x: [minHash(x)]).flatMap(lambda x:x.values()).filter(lambda x: len(x)>1)
        combinations=candidatePairs.flatMap(lambda lst: [list(x) for x in itertools.combinations(lst, 2)]).groupBy(lambda x: ((x[0],x[1]))).map(lambda (x,y):x)
        combinationsRdd=sc.parallelize(combinations.collect(),1000)
        userSet=sc.broadcast(userSetRdd.collect())
        similarityRdd=combinationsRdd.flatMap(lambda x: findJaccardSimilarity(x,userSet))
        topFiveRdd=similarityRdd.groupByKey().map(lambda (x,y): (x,getTopFive(y))).map(lambda (x,items): "U"+str(x+1)+":"+",".join(["U"+str(y[0]+1) for y in items])).sortBy(lambda x: int(x.split(":")[0].split("U")[1]))
        output=topFiveRdd.collect()

        f=open(outputFile, 'w')
        for item in output:
            f.write("%s\n" % item)
    except Exception,e:
        print(str(e))
        print('Syntax:')
        print('\tpython Onzali_Suba_lshrec.py <inputFile> <outputFile>')
        sys.exit()

def computeSignatures(userSet,row):
    movie,users=userSet
    hashValue=(3*(movie) + 13*row) % 100
    return [(user,hashValue) for user in users]

def minHash(x):
    x=list(x)
    y=zip(x[0],x[1],x[2],x[3])
    candidatePairs=defaultdict(list)
    for i in xrange(0,len(y)):
        candidatePairs[y[i]].append(i)
    return candidatePairs

def compareSimilarity(a,b):
    com=cmp(b[1],a[1])
    return cmp(a[0],b[0]) if com==0 else com

def getTopFive(x):
    x=list(x)
    x.sort(compareSimilarity)
    length=5 if len(x)>=5 else len(x)
    result=x[:length]
    result.sort()
    return result    

def findJaccardSimilarity(x,userSet):
    userSet1=dict(userSet.value)
    a=len(set(userSet1[x[0]+1]).intersection(userSet1[x[1]+1]))
    b=len(set(userSet1[x[0]+1]).union(userSet1[x[1]+1]))
    return [(x[0],(x[1],float(a)/float(b))),(x[1],(x[0],float(a)/float(b)))]

if __name__ == "__main__":
    main()
