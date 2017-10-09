import csv
import sys
import numpy as np
import math
import heapq
from scipy import spatial
from collections import defaultdict
import collections


def flatten(x):
    if isinstance(x, collections.Iterable):
        return [a for i in x for a in flatten(i)]
    else:
        return [x]
    
    
def main():
    try:
        inputFile=sys.argv[1]
        number_of_clusters = int(sys.argv[2])
        f=open(inputFile)
        number_of_documents=int(f.readline())
        number_of_Words=int(f.readline())
        vector_matrix=np.array([[0.0 for i in range(number_of_Words+1)] for j in range(number_of_documents+1)])
        number_of_words_in_one_doc=int(f.readline())
        with f:
            for line in f:
                doc,word,freq=line.split(" ")
                vector_matrix[int(doc),int(word)]=float(freq)
                
        df_matrix=np.array([0.0 for j in range(number_of_Words+1)])
        for i in range(1,number_of_Words+1):
            df_matrix[i]=math.log(float(number_of_documents+1)/float(len(np.where(vector_matrix[:,i]!=0)[0])+1),2)
            
        for i in range(1,number_of_documents+1):
            vector_matrix[i,:]=np.multiply(vector_matrix[i,:],df_matrix)
            euclidean_distance=np.linalg.norm(vector_matrix[i,:])
            vector_matrix[i:]=np.divide(vector_matrix[i:],euclidean_distance)
            
        myHeap=[]
        cluster_sum=defaultdict()
        cluster_count=defaultdict()
        cluster_centroid=defaultdict()
        
        for i in range(1,number_of_documents):
            for j in range(i+1,number_of_documents+1):
                cosine_distance=spatial.distance.cosine(vector_matrix[i],vector_matrix[j])
                heapq.heappush(myHeap,(cosine_distance,(i,j)))
                cluster_sum[i]=vector_matrix[i]
                cluster_sum[j]=vector_matrix[j]
                cluster_count[i]=1
                cluster_count[j]=1
                cluster_centroid[i]=np.divide(cluster_sum[i],cluster_count[i])
                cluster_centroid[j]=np.divide(cluster_sum[j],cluster_count[j])
               
        cluster_keys=cluster_centroid.keys()
        while len(cluster_keys)>5:
            cluster=heapq.heappop(myHeap)
            if cluster[1][0] not in cluster_keys or cluster[1][1] not in cluster_keys:
                continue
            cluster_sum[cluster[1]]=np.add(cluster_sum[cluster[1][0]],cluster_sum[cluster[1][1]])
            del cluster_sum[cluster[1][0]]
            del cluster_sum[cluster[1][1]]
            cluster_count[cluster[1]]=cluster_count[cluster[1][0]]+cluster_count[cluster[1][1]]
            del cluster_count[cluster[1][0]]
            del cluster_count[cluster[1][1]]
            cluster_centroid[cluster[1]]=np.divide(cluster_sum[cluster[1]],cluster_count[cluster[1]])
            del cluster_centroid[cluster[1][0]]
            del cluster_centroid[cluster[1][1]]
            cluster_keys=cluster_centroid.keys()
            for key in cluster_centroid.keys():
                if key != cluster[1]:
                    cosine_distance=spatial.distance.cosine(cluster_centroid[cluster[1]],cluster_centroid[key])
                    heapq.heappush(myHeap,(cosine_distance,(cluster[1],key)))
                    
        for k in cluster_centroid.keys():
            clust=flatten(k)
            clust.sort()
            print(", ".join(str(e) for e in clust))
   
    except Exception,e:
        print(str(e))
        print('Syntax:')
        print('\tpython Onzali_Suba_hac.py <inputFile> number_of_clusters')
        sys.exit()

if __name__ == "__main__":
    main()
