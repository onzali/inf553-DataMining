from collections import defaultdict
from pyspark import SparkContext
from collections import defaultdict
from itertools import chain, combinations
import sys

def main():
    try:
        inputFile=sys.argv[1]
        supportThreshold=float(sys.argv[2])
        outputFile=sys.argv[3]
        sc = SparkContext()

        numPartitions=sc.broadcast(2)

        data=sc.textFile("baskets.txt",numPartitions.value)
        count=data.count()
        support=sc.broadcast(supportThreshold*count)

        data=data.map(lambda d: [int(i) for i in d.split(',')])

        phaseOneOutput=data.mapPartitions(lambda part: [freqSet for freqSet in localApriori(part,support,numPartitions).values()]).flatMap(lambda freqSet: [(freqItem,1) for freqItem in freqSet]).reduceByKey(lambda x,y: x).map(lambda x: x[0]).collect()

        candidateItems=sc.broadcast(phaseOneOutput)

        phaseTwoOutput=data.flatMap(lambda basket: globalFreq(basket,candidateItems)).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1]>=support.value).map(lambda (freqSet,count): freqSet).map(lambda freqSet: ",".join([str(item) for item in freqSet])).collect()

        f=open(outputFile, 'w')
        for item in phaseTwoOutput:
            f.write("%s\n" % item)
    except Exception,e:
        print(str(e))
        print('Syntax:')
        print('\tpython son.py <inputFile> <support> <outputFile>')
        sys.exit()

def localApriori(iterator,support,numPartitions):
    
    results=defaultdict(list)
    itemCount=defaultdict(int)
    frequentSet=set()
    baskets=[]
    for v in iterator:
        basket=set(v)
        baskets.append(basket)
        for item in basket:
            itemCount[item]+=1;
    
    minSupport=support.value/numPartitions.value
    k=1
    breakLoop=False
    while (not breakLoop):
        if(k>1):
            combSet=[set(itemset) for itemset in chain(*[combinations(expandedItems, k)])]
            itemCount=defaultdict(int)
            for item in combSet:
                for basket in baskets:
                    if item.issubset(basket):
                        itemCount[frozenset(item)] += 1
        frequentSet=set()
        for item in itemCount:
            if(itemCount[item]>=minSupport):
                frequentSet.add(item)
        if(frequentSet):
            if(k==1):
                results[k]=[frozenset([item]) for item in frequentSet]
            else:
                results[k]=frequentSet
            k+=1
            if(k>2):
                expandedItems = set([item for tup in frequentSet for item in tup])
            else:
                expandedItems = frequentSet
        else:
            breakLoop=True
    
    return results

def globalFreq(basket,candidateItems):
    candidateItemsValue=candidateItems.value
    results=[]
    for candidateItem in candidateItemsValue:
        if candidateItem.issubset(basket):
            results.append((candidateItem,1))
    return results

if __name__ == "__main__":
    main()