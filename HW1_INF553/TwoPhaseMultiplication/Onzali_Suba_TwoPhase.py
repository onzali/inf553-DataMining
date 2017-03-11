from collections import defaultdict
from pyspark import SparkContext
from collections import defaultdict
import sys

def main():
        try:
            matA=sys.argv[1]
            matB=sys.argv[2]
            outputFile=sys.argv[3]
            sc = SparkContext()

            mapA=sc.textFile(matA).map(lambda x: (x.split(',')[1],('A',(x.split(',')[0],x.split(',')[2]))))

            mapB=sc.textFile(matB).map(lambda x: (x.split(',')[0],('B',(x.split(',')[1],x.split(',')[2]))))

            ABcogroup=mapA.cogroup(mapB)

            result=ABcogroup.mapValues(reducePhaseOne).map(lambda x:x[1]).flatMap(lambda x: x.items()).reduceByKey(lambda x,y:x+y).map(lambda x:str(x[0][0])+","+str(x[0][1])+"\t"+str(x[1])).coalesce(1).collect()
            f=open(outputFile, 'w')
            for item in result:
                f.write("%s\n" % item)
        except Exception,e:
            print(str(e))
            print('Syntax:')
            print('\tpython TwoPhase.py <matAFile> <matBFile> <outputFile>')
            sys.exit()

def reducePhaseOne(x):
    a = list(x[0])
    b=list(x[1])
    d=defaultdict(int)
    for i in xrange(0,len(a)):
        for j in xrange(0,len(b)):
            d[(int(a[i][1][0]),int(b[j][1][0]))]=int(a[i][1][1])*int(b[j][1][1])
    return d

if __name__ == "__main__":
    main()